package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	capacityHistoryRetention    = 7 * 24 * time.Hour
	capacityHistoryRowLimit     = 500_000
	connectionCapacityRowLimit  = 50_000
	capacitySampleInterval      = time.Minute
	capacityForecastMaxSamples  = 240
	capacityForecastMinSamples  = 4
	capacityForecastMinSpan     = time.Minute
	capacityForecastDefaultSpan = time.Hour
	capacityForecastMaxSpan     = 7 * 24 * time.Hour
)

type capacitySchemaState struct {
	mu    sync.Mutex
	ready bool
}

var capacitySchemaStates sync.Map // map[*store]*capacitySchemaState

func (s *store) ensureCapacitySchema(ctx context.Context) error {
	value, _ := capacitySchemaStates.LoadOrStore(s, &capacitySchemaState{})
	state := value.(*capacitySchemaState)
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.ready {
		return nil
	}
	statements := []string{
		`CREATE TABLE IF NOT EXISTS stream_capacity_samples (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				recorded_at TEXT NOT NULL,
				connection_id TEXT NOT NULL,
				stream_key TEXT NOT NULL,
				length INTEGER NOT NULL,
				memory_bytes INTEGER,
				entries_added INTEGER NOT NULL,
				removed_entries_estimate INTEGER NOT NULL,
				retention_window_ms INTEGER,
				UNIQUE(connection_id, stream_key, recorded_at)
			)`,
		`CREATE INDEX IF NOT EXISTS stream_capacity_scope_time_idx
				ON stream_capacity_samples(connection_id, stream_key, recorded_at DESC)`,
		`CREATE TABLE IF NOT EXISTS connection_capacity_samples (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				recorded_at TEXT NOT NULL,
				connection_id TEXT NOT NULL,
				used_memory_bytes INTEGER NOT NULL,
				max_memory_bytes INTEGER NOT NULL,
				UNIQUE(connection_id, recorded_at)
			)`,
		`CREATE INDEX IF NOT EXISTS connection_capacity_scope_time_idx
				ON connection_capacity_samples(connection_id, recorded_at DESC)`,
		`CREATE TABLE IF NOT EXISTS stream_retention_policies (
				connection_id TEXT NOT NULL,
				stream_key TEXT NOT NULL,
				max_length INTEGER,
				max_age_seconds INTEGER,
				require_trimming INTEGER NOT NULL DEFAULT 0,
				updated_by TEXT NOT NULL DEFAULT '',
				created_at TEXT NOT NULL,
				updated_at TEXT NOT NULL,
				PRIMARY KEY(connection_id, stream_key)
			)`,
	}
	for _, statement := range statements {
		if _, err := s.db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("capacity schema: %w", err)
		}
	}
	state.ready = true
	return nil
}

func (s *store) recordCapacitySample(ctx context.Context, snapshot operationalSnapshot) error {
	if snapshot.StreamSampledAt == nil || !snapshot.Up {
		return nil
	}
	if err := s.ensureCapacitySchema(ctx); err != nil {
		return err
	}
	recordedAt := snapshot.StreamSampledAt.UTC().Format(time.RFC3339Nano)
	transaction, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer transaction.Rollback()
	var latestSample sql.NullString
	if err := transaction.QueryRowContext(ctx, `SELECT MAX(recorded_at) FROM connection_capacity_samples WHERE connection_id=?`, snapshot.ConnectionID).Scan(&latestSample); err != nil {
		return err
	}
	if latestSample.Valid {
		if latest, parseErr := time.Parse(time.RFC3339Nano, latestSample.String); parseErr == nil && snapshot.StreamSampledAt.Sub(latest) < capacitySampleInterval {
			return transaction.Commit()
		}
	}
	for _, stream := range snapshot.Streams {
		if !stream.Available {
			continue
		}
		if _, err := transaction.ExecContext(ctx, `INSERT OR IGNORE INTO stream_capacity_samples(
			recorded_at, connection_id, stream_key, length, memory_bytes, entries_added,
			removed_entries_estimate, retention_window_ms
		) VALUES(?,?,?,?,?,?,?,?)`, recordedAt, snapshot.ConnectionID, stream.Key, stream.Length,
			nullableInt64(stream.MemoryBytes), stream.EntriesAdded, stream.RemovedEntriesEstimate,
			nullableInt64(stream.RetentionWindowMs)); err != nil {
			return err
		}
	}
	if snapshot.NodeSampledAt != nil {
		if _, err := transaction.ExecContext(ctx, `INSERT OR IGNORE INTO connection_capacity_samples(
			recorded_at, connection_id, used_memory_bytes, max_memory_bytes
		) VALUES(?,?,?,?)`, snapshot.NodeSampledAt.UTC().Format(time.RFC3339Nano), snapshot.ConnectionID,
			snapshot.Memory.UsedBytes, snapshot.Memory.MaxBytes); err != nil {
			return err
		}
	}
	return transaction.Commit()
}

type retentionPolicy struct {
	ConnectionID    string    `json:"connectionId"`
	StreamKey       string    `json:"streamKey"`
	MaxLength       *int64    `json:"maxLength,omitempty"`
	MaxAgeSeconds   *int64    `json:"maxAgeSeconds,omitempty"`
	RequireTrimming bool      `json:"requireTrimming"`
	UpdatedBy       string    `json:"updatedBy,omitempty"`
	CreatedAt       time.Time `json:"createdAt"`
	UpdatedAt       time.Time `json:"updatedAt"`
}

func validateRetentionPolicy(policy retentionPolicy) error {
	policy.StreamKey = strings.TrimSpace(policy.StreamKey)
	if policy.StreamKey == "" || len(policy.StreamKey) > 512 {
		return errors.New("streamKey is required and must not exceed 512 characters")
	}
	if policy.MaxLength != nil && *policy.MaxLength <= 0 {
		return errors.New("maxLength must be greater than zero")
	}
	if policy.MaxAgeSeconds != nil && *policy.MaxAgeSeconds <= 0 {
		return errors.New("maxAgeSeconds must be greater than zero")
	}
	if policy.MaxLength == nil && policy.MaxAgeSeconds == nil && !policy.RequireTrimming {
		return errors.New("at least one retention expectation is required")
	}
	return nil
}

func (s *store) upsertRetentionPolicy(ctx context.Context, policy retentionPolicy) (retentionPolicy, error) {
	policy.ConnectionID = strings.TrimSpace(policy.ConnectionID)
	policy.StreamKey = strings.TrimSpace(policy.StreamKey)
	if policy.ConnectionID == "" {
		return retentionPolicy{}, errors.New("connectionId is required")
	}
	if err := validateRetentionPolicy(policy); err != nil {
		return retentionPolicy{}, err
	}
	if err := s.ensureCapacitySchema(ctx); err != nil {
		return retentionPolicy{}, err
	}
	now := time.Now().UTC()
	_, err := s.db.ExecContext(ctx, `INSERT INTO stream_retention_policies(
		connection_id, stream_key, max_length, max_age_seconds, require_trimming,
		updated_by, created_at, updated_at
	) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(connection_id, stream_key) DO UPDATE SET
		max_length=excluded.max_length, max_age_seconds=excluded.max_age_seconds,
		require_trimming=excluded.require_trimming, updated_by=excluded.updated_by,
		updated_at=excluded.updated_at`, policy.ConnectionID, policy.StreamKey,
		nullableInt64(policy.MaxLength), nullableInt64(policy.MaxAgeSeconds), boolToInt(policy.RequireTrimming),
		policy.UpdatedBy, now.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano))
	if err != nil {
		return retentionPolicy{}, err
	}
	return s.retentionPolicy(ctx, policy.ConnectionID, policy.StreamKey)
}

func (s *store) retentionPolicy(ctx context.Context, connectionID, streamKey string) (retentionPolicy, error) {
	if err := s.ensureCapacitySchema(ctx); err != nil {
		return retentionPolicy{}, err
	}
	var item retentionPolicy
	var maxLength, maxAge sql.NullInt64
	var requireTrimming int
	var createdAt, updatedAt string
	err := s.db.QueryRowContext(ctx, `SELECT connection_id, stream_key, max_length, max_age_seconds,
		require_trimming, updated_by, created_at, updated_at FROM stream_retention_policies
		WHERE connection_id=? AND stream_key=?`, connectionID, streamKey).Scan(&item.ConnectionID,
		&item.StreamKey, &maxLength, &maxAge, &requireTrimming, &item.UpdatedBy, &createdAt, &updatedAt)
	if err != nil {
		return retentionPolicy{}, err
	}
	if maxLength.Valid {
		item.MaxLength = int64Pointer(maxLength.Int64)
	}
	if maxAge.Valid {
		item.MaxAgeSeconds = int64Pointer(maxAge.Int64)
	}
	item.RequireTrimming = requireTrimming == 1
	item.CreatedAt, _ = time.Parse(time.RFC3339Nano, createdAt)
	item.UpdatedAt, _ = time.Parse(time.RFC3339Nano, updatedAt)
	return item, nil
}

func (s *store) listRetentionPolicies(ctx context.Context, connectionID string) ([]retentionPolicy, error) {
	if err := s.ensureCapacitySchema(ctx); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT connection_id, stream_key, max_length, max_age_seconds,
		require_trimming, updated_by, created_at, updated_at FROM stream_retention_policies
		WHERE connection_id=? ORDER BY stream_key LIMIT ?`, connectionID, operationalMaxStreams)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]retentionPolicy, 0)
	for rows.Next() {
		var item retentionPolicy
		var maxLength, maxAge sql.NullInt64
		var trimming int
		var createdAt, updatedAt string
		if err := rows.Scan(&item.ConnectionID, &item.StreamKey, &maxLength, &maxAge, &trimming,
			&item.UpdatedBy, &createdAt, &updatedAt); err != nil {
			return nil, err
		}
		if maxLength.Valid {
			item.MaxLength = int64Pointer(maxLength.Int64)
		}
		if maxAge.Valid {
			item.MaxAgeSeconds = int64Pointer(maxAge.Int64)
		}
		item.RequireTrimming = trimming == 1
		item.CreatedAt, _ = time.Parse(time.RFC3339Nano, createdAt)
		item.UpdatedAt, _ = time.Parse(time.RFC3339Nano, updatedAt)
		result = append(result, item)
	}
	return result, rows.Err()
}

func (s *store) deleteRetentionPolicy(ctx context.Context, connectionID, streamKey string) (bool, error) {
	if err := s.ensureCapacitySchema(ctx); err != nil {
		return false, err
	}
	result, err := s.db.ExecContext(ctx, `DELETE FROM stream_retention_policies WHERE connection_id=? AND stream_key=?`, connectionID, streamKey)
	if err != nil {
		return false, err
	}
	affected, _ := result.RowsAffected()
	return affected > 0, nil
}

type trendPoint struct {
	At    time.Time
	Value float64
}

type robustTrend struct {
	Slope      *float64
	Confidence float64
	Samples    int
	Span       time.Duration
}

// robustTrendFor uses a bounded Theil-Sen slope and a median-residual score.
// It tolerates isolated Redis sampling spikes without turning them into a
// confident capacity prediction.
func robustTrendFor(points []trendPoint) robustTrend {
	if len(points) > capacityForecastMaxSamples {
		step := float64(len(points)-1) / float64(capacityForecastMaxSamples-1)
		reduced := make([]trendPoint, 0, capacityForecastMaxSamples)
		for index := 0; index < capacityForecastMaxSamples; index++ {
			reduced = append(reduced, points[int(math.Round(float64(index)*step))])
		}
		points = reduced
	}
	result := robustTrend{Samples: len(points)}
	if len(points) < capacityForecastMinSamples {
		return result
	}
	sort.Slice(points, func(i, j int) bool { return points[i].At.Before(points[j].At) })
	result.Span = points[len(points)-1].At.Sub(points[0].At)
	if result.Span < capacityForecastMinSpan {
		return result
	}
	slopes := make([]float64, 0, len(points)*(len(points)-1)/2)
	for left := 0; left < len(points); left++ {
		for right := left + 1; right < len(points); right++ {
			seconds := points[right].At.Sub(points[left].At).Seconds()
			if seconds > 0 {
				slopes = append(slopes, (points[right].Value-points[left].Value)/seconds)
			}
		}
	}
	if len(slopes) == 0 {
		return result
	}
	slope := medianFloat64(slopes)
	result.Slope = &slope
	intercepts := make([]float64, 0, len(points))
	base := points[0].At
	for _, point := range points {
		intercepts = append(intercepts, point.Value-slope*point.At.Sub(base).Seconds())
	}
	intercept := medianFloat64(intercepts)
	residuals := make([]float64, 0, len(points))
	values := make([]float64, 0, len(points))
	for _, point := range points {
		predicted := intercept + slope*point.At.Sub(base).Seconds()
		residuals = append(residuals, math.Abs(point.Value-predicted))
		values = append(values, point.Value)
	}
	valueMedian := medianFloat64(values)
	deviations := make([]float64, 0, len(values))
	for _, value := range values {
		deviations = append(deviations, math.Abs(value-valueMedian))
	}
	scale := medianFloat64(deviations)
	if scale < 1 {
		scale = math.Max(1, math.Abs(valueMedian)*0.01)
	}
	fit := 1 - math.Min(1, medianFloat64(residuals)/(scale*2))
	sampleConfidence := math.Min(1, float64(len(points))/12)
	spanConfidence := math.Min(1, result.Span.Seconds()/(15*time.Minute).Seconds())
	result.Confidence = clampFloat64(fit*sampleConfidence*spanConfidence, 0, 1)
	return result
}

func medianFloat64(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	ordered := append([]float64(nil), values...)
	sort.Float64s(ordered)
	middle := len(ordered) / 2
	if len(ordered)%2 == 0 {
		return (ordered[middle-1] + ordered[middle]) / 2
	}
	return ordered[middle]
}

func clampFloat64(value, minimum, maximum float64) float64 {
	if value < minimum {
		return minimum
	}
	if value > maximum {
		return maximum
	}
	return value
}

func projectedValue(current float64, trend robustTrend, horizon time.Duration) *float64 {
	if trend.Slope == nil {
		return nil
	}
	value := current + *trend.Slope*horizon.Seconds()
	if value < 0 {
		value = 0
	}
	return &value
}

type capacityProjection struct {
	Current            int64    `json:"current"`
	GrowthPerSecond    *float64 `json:"growthPerSecond,omitempty"`
	ProjectedIn1Hour   *float64 `json:"projectedIn1Hour,omitempty"`
	ProjectedIn24Hours *float64 `json:"projectedIn24Hours,omitempty"`
	Confidence         float64  `json:"confidence"`
	SampleCount        int      `json:"sampleCount"`
	HistorySpanSeconds float64  `json:"historySpanSeconds"`
}

type retentionCompliance struct {
	Status                   string   `json:"status"`
	Reasons                  []string `json:"reasons"`
	TrimObserved             bool     `json:"trimObserved"`
	ObservedRetentionSeconds *float64 `json:"observedRetentionSeconds,omitempty"`
	PolicySource             string   `json:"policySource"`
}

type streamCapacityForecast struct {
	StreamKey   string              `json:"streamKey"`
	Length      capacityProjection  `json:"length"`
	MemoryBytes *capacityProjection `json:"memoryBytes,omitempty"`
	Policy      *retentionPolicy    `json:"policy,omitempty"`
	Compliance  retentionCompliance `json:"compliance"`
}

type connectionCapacityForecast struct {
	UsedMemoryBytes        capacityProjection `json:"usedMemoryBytes"`
	MaxMemoryBytes         int64              `json:"maxMemoryBytes"`
	TimeToMaxMemorySeconds *float64           `json:"timeToMaxMemorySeconds,omitempty"`
}

type capacityForecastResponse struct {
	ConnectionID  string                     `json:"connectionId"`
	SampledAt     time.Time                  `json:"sampledAt"`
	WindowSeconds float64                    `json:"windowSeconds"`
	Connection    connectionCapacityForecast `json:"connection"`
	Streams       []streamCapacityForecast   `json:"streams"`
	Disclaimer    string                     `json:"disclaimer"`
}

type capacityRawSample struct {
	At              time.Time
	Length          int64
	MemoryBytes     *int64
	EntriesAdded    int64
	RemovedEstimate int64
	RetentionMs     *int64
}

func projectionFromPoints(current int64, points []trendPoint) capacityProjection {
	trend := robustTrendFor(points)
	return capacityProjection{
		Current: current, GrowthPerSecond: trend.Slope,
		ProjectedIn1Hour:   projectedValue(float64(current), trend, time.Hour),
		ProjectedIn24Hours: projectedValue(float64(current), trend, 24*time.Hour),
		Confidence:         trend.Confidence, SampleCount: trend.Samples, HistorySpanSeconds: trend.Span.Seconds(),
	}
}

func (s *store) capacityForecast(ctx context.Context, connectionID string, from, to time.Time) (capacityForecastResponse, error) {
	if err := s.ensureCapacitySchema(ctx); err != nil {
		return capacityForecastResponse{}, err
	}
	result := capacityForecastResponse{
		ConnectionID: connectionID, WindowSeconds: to.Sub(from).Seconds(), Streams: []streamCapacityForecast{},
		Disclaimer: "Retention policies are user-defined expectations. Redis does not expose a persistent XADD MAXLEN setting.",
	}
	connectionSamples, err := s.loadConnectionCapacitySamples(ctx, connectionID, from, to)
	if err != nil {
		return result, err
	}
	if len(connectionSamples) > 0 {
		latest := connectionSamples[len(connectionSamples)-1]
		result.SampledAt = latest.At
		points := make([]trendPoint, 0, len(connectionSamples))
		for _, sample := range connectionSamples {
			points = append(points, trendPoint{At: sample.At, Value: float64(sample.Used)})
		}
		projection := projectionFromPoints(latest.Used, points)
		result.Connection.UsedMemoryBytes = projection
		result.Connection.MaxMemoryBytes = latest.Max
		if latest.Max > latest.Used && projection.GrowthPerSecond != nil && *projection.GrowthPerSecond > 0 && projection.Confidence >= 0.25 {
			seconds := float64(latest.Max-latest.Used) / *projection.GrowthPerSecond
			result.Connection.TimeToMaxMemorySeconds = &seconds
		}
	}
	streamSamples, err := s.loadStreamCapacitySamples(ctx, connectionID, from, to)
	if err != nil {
		return result, err
	}
	policies, err := s.listRetentionPolicies(ctx, connectionID)
	if err != nil {
		return result, err
	}
	policyByStream := make(map[string]retentionPolicy, len(policies))
	for _, policy := range policies {
		policyByStream[policy.StreamKey] = policy
	}
	keys := make([]string, 0, len(streamSamples))
	for key := range streamSamples {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		samples := streamSamples[key]
		if len(samples) == 0 {
			continue
		}
		latest := samples[len(samples)-1]
		lengthPoints := make([]trendPoint, 0, len(samples))
		memoryPoints := make([]trendPoint, 0, len(samples))
		for _, sample := range samples {
			lengthPoints = append(lengthPoints, trendPoint{At: sample.At, Value: float64(sample.Length)})
			if sample.MemoryBytes != nil {
				memoryPoints = append(memoryPoints, trendPoint{At: sample.At, Value: float64(*sample.MemoryBytes)})
			}
		}
		item := streamCapacityForecast{StreamKey: key, Length: projectionFromPoints(latest.Length, lengthPoints)}
		if latest.MemoryBytes != nil {
			projection := projectionFromPoints(*latest.MemoryBytes, memoryPoints)
			item.MemoryBytes = &projection
		}
		if policy, exists := policyByStream[key]; exists {
			policyCopy := policy
			item.Policy = &policyCopy
		}
		item.Compliance = evaluateRetentionCompliance(samples, item.Length, item.Policy)
		result.Streams = append(result.Streams, item)
	}
	return result, nil
}

type connectionCapacitySample struct {
	At   time.Time
	Used int64
	Max  int64
}

func (s *store) loadConnectionCapacitySamples(ctx context.Context, connectionID string, from, to time.Time) ([]connectionCapacitySample, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT recorded_at, used_memory_bytes, max_memory_bytes
		FROM connection_capacity_samples WHERE connection_id=? AND recorded_at>=? AND recorded_at<=?
		ORDER BY recorded_at DESC LIMIT ?`, connectionID, from.Format(time.RFC3339Nano), to.Format(time.RFC3339Nano), capacityForecastMaxSamples)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]connectionCapacitySample, 0)
	for rows.Next() {
		var item connectionCapacitySample
		var at string
		if err := rows.Scan(&at, &item.Used, &item.Max); err != nil {
			return nil, err
		}
		item.At, _ = time.Parse(time.RFC3339Nano, at)
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for left, right := 0, len(result)-1; left < right; left, right = left+1, right-1 {
		result[left], result[right] = result[right], result[left]
	}
	return result, nil
}

func (s *store) loadStreamCapacitySamples(ctx context.Context, connectionID string, from, to time.Time) (map[string][]capacityRawSample, error) {
	rows, err := s.db.QueryContext(ctx, `WITH ranked AS (
		SELECT recorded_at, stream_key, length, memory_bytes, entries_added,
			removed_entries_estimate, retention_window_ms,
			ROW_NUMBER() OVER (PARTITION BY stream_key ORDER BY recorded_at DESC) AS sample_rank
		FROM stream_capacity_samples
		WHERE connection_id=? AND recorded_at>=? AND recorded_at<=?
	) SELECT recorded_at, stream_key, length, memory_bytes, entries_added,
		removed_entries_estimate, retention_window_ms FROM ranked
		WHERE sample_rank<=? ORDER BY stream_key, recorded_at`,
		connectionID, from.Format(time.RFC3339Nano), to.Format(time.RFC3339Nano), capacityForecastMaxSamples)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make(map[string][]capacityRawSample)
	for rows.Next() {
		var item capacityRawSample
		var at, key string
		var memory, retention sql.NullInt64
		if err := rows.Scan(&at, &key, &item.Length, &memory, &item.EntriesAdded, &item.RemovedEstimate, &retention); err != nil {
			return nil, err
		}
		item.At, _ = time.Parse(time.RFC3339Nano, at)
		if memory.Valid {
			item.MemoryBytes = int64Pointer(memory.Int64)
		}
		if retention.Valid {
			item.RetentionMs = int64Pointer(retention.Int64)
		}
		items := result[key]
		if len(items) >= capacityForecastMaxSamples {
			// Keep a deterministic bounded window for each stream.
			items = items[1:]
		}
		result[key] = append(items, item)
	}
	return result, rows.Err()
}

func evaluateRetentionCompliance(samples []capacityRawSample, length capacityProjection, policy *retentionPolicy) retentionCompliance {
	result := retentionCompliance{Status: "unconfigured", Reasons: []string{}, PolicySource: "user_defined_expectation"}
	if policy == nil || len(samples) == 0 {
		return result
	}
	result.Status = "compliant"
	latest := samples[len(samples)-1]
	if latest.RetentionMs != nil {
		seconds := float64(*latest.RetentionMs) / 1000
		result.ObservedRetentionSeconds = &seconds
	}
	for index := 1; index < len(samples); index++ {
		if samples[index].RemovedEstimate > samples[index-1].RemovedEstimate {
			result.TrimObserved = true
			break
		}
	}
	violation := false
	atRisk := false
	if policy.MaxLength != nil {
		if latest.Length > *policy.MaxLength {
			violation = true
			result.Reasons = append(result.Reasons, "length_exceeds_expected_max")
		} else if length.ProjectedIn1Hour != nil && *length.ProjectedIn1Hour > float64(*policy.MaxLength) {
			atRisk = true
			result.Reasons = append(result.Reasons, "projected_length_exceeds_expected_max_within_1h")
		}
	}
	if policy.MaxAgeSeconds != nil && result.ObservedRetentionSeconds != nil && *result.ObservedRetentionSeconds > float64(*policy.MaxAgeSeconds) {
		violation = true
		result.Reasons = append(result.Reasons, "observed_age_exceeds_expected_max")
	}
	if policy.RequireTrimming && len(samples) >= capacityForecastMinSamples {
		entriesIncreased := latest.EntriesAdded > samples[0].EntriesAdded
		if entriesIncreased && !result.TrimObserved {
			violation = true
			result.Reasons = append(result.Reasons, "no_trim_observed_while_entries_grew")
		}
	}
	if violation {
		result.Status = "violating"
	} else if atRisk {
		result.Status = "at_risk"
	}
	return result
}

func maintainCapacityHistoryTx(ctx context.Context, transaction *sql.Tx, now time.Time) error {
	cutoff := now.Add(-capacityHistoryRetention).Format(time.RFC3339Nano)
	for _, table := range []string{"stream_capacity_samples", "connection_capacity_samples"} {
		if _, err := transaction.ExecContext(ctx, `DELETE FROM `+table+` WHERE recorded_at<?`, cutoff); err != nil {
			// Older databases may call the shared retention helper before the
			// capacity schema has been initialized.
			if strings.Contains(strings.ToLower(err.Error()), "no such table") {
				continue
			}
			return err
		}
	}
	for table, limit := range map[string]int{"stream_capacity_samples": capacityHistoryRowLimit, "connection_capacity_samples": connectionCapacityRowLimit} {
		query := fmt.Sprintf(`DELETE FROM %s WHERE id IN (SELECT id FROM %s ORDER BY id DESC LIMIT -1 OFFSET ?)`, table, table)
		if _, err := transaction.ExecContext(ctx, query, limit); err != nil {
			if strings.Contains(strings.ToLower(err.Error()), "no such table") {
				continue
			}
			return err
		}
	}
	return nil
}

func parseCapacityWindow(raw string) (time.Duration, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return capacityForecastDefaultSpan, nil
	}
	value, err := time.ParseDuration(raw)
	if err != nil || value < capacityForecastMinSpan || value > capacityForecastMaxSpan {
		return 0, fmt.Errorf("window must be between %s and %s", capacityForecastMinSpan, capacityForecastMaxSpan)
	}
	return value, nil
}

func (s *apiServer) capacityForecast(writer http.ResponseWriter, request *http.Request) {
	connection, err := s.redis.get(request.URL.Query().Get("connectionId"))
	if err != nil {
		writeError(writer, http.StatusBadRequest, "unknown_connection", err.Error())
		return
	}
	window, err := parseCapacityWindow(request.URL.Query().Get("window"))
	if err != nil {
		writeError(writer, http.StatusBadRequest, "invalid_window", err.Error())
		return
	}
	checker, err := s.streamPermissionChecker(request, "streams:read")
	if err != nil {
		writePermissionCheckError(writer)
		return
	}
	to := time.Now().UTC()
	result, err := s.store.capacityForecast(request.Context(), connection.config.ID, to.Add(-window), to)
	if err != nil {
		writeError(writer, http.StatusInternalServerError, "capacity_forecast_failed", err.Error())
		return
	}
	visible := make([]streamCapacityForecast, 0, len(result.Streams))
	for _, stream := range result.Streams {
		if checker.allows("streams:read", redisStreamScope(connection.config.ID, stream.StreamKey)) {
			visible = append(visible, stream)
		}
	}
	result.Streams = visible
	writeJSON(writer, http.StatusOK, result)
}

func (s *apiServer) retentionPolicies(writer http.ResponseWriter, request *http.Request) {
	connection, err := s.redis.get(request.URL.Query().Get("connectionId"))
	if err != nil {
		writeError(writer, http.StatusBadRequest, "unknown_connection", err.Error())
		return
	}
	checker, err := s.streamPermissionChecker(request, "streams:read")
	if err != nil {
		writePermissionCheckError(writer)
		return
	}
	items, err := s.store.listRetentionPolicies(request.Context(), connection.config.ID)
	if err != nil {
		writeError(writer, http.StatusInternalServerError, "retention_policies_failed", err.Error())
		return
	}
	visible := make([]retentionPolicy, 0, len(items))
	for _, item := range items {
		if checker.allows("streams:read", redisStreamScope(connection.config.ID, item.StreamKey)) {
			visible = append(visible, item)
		}
	}
	writeJSON(writer, http.StatusOK, map[string]any{"items": visible})
}

func (s *apiServer) putRetentionPolicy(writer http.ResponseWriter, request *http.Request) {
	var input struct {
		MaxLength       *int64 `json:"maxLength"`
		MaxAgeSeconds   *int64 `json:"maxAgeSeconds"`
		RequireTrimming bool   `json:"requireTrimming"`
	}
	if err := readJSON(request, &input, 16<<10); err != nil {
		writeError(writer, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	connection, err := s.redis.get(request.URL.Query().Get("connectionId"))
	if err != nil {
		writeError(writer, http.StatusBadRequest, "unknown_connection", err.Error())
		return
	}
	session, _ := request.Context().Value(sessionContextKey).(sessionRecord)
	item, err := s.store.upsertRetentionPolicy(request.Context(), retentionPolicy{
		ConnectionID: connection.config.ID, StreamKey: request.URL.Query().Get("streamKey"), MaxLength: input.MaxLength,
		MaxAgeSeconds: input.MaxAgeSeconds, RequireTrimming: input.RequireTrimming, UpdatedBy: session.UserID,
	})
	if err != nil {
		writeError(writer, http.StatusBadRequest, "invalid_retention_policy", err.Error())
		return
	}
	writeJSON(writer, http.StatusOK, item)
}

func (s *apiServer) deleteRetentionPolicy(writer http.ResponseWriter, request *http.Request) {
	connection, err := s.redis.get(request.URL.Query().Get("connectionId"))
	if err != nil {
		writeError(writer, http.StatusBadRequest, "unknown_connection", err.Error())
		return
	}
	streamKey := strings.TrimSpace(firstNonEmpty(request.URL.Query().Get("streamKey"), request.URL.Query().Get("stream")))
	if streamKey == "" {
		writeError(writer, http.StatusBadRequest, "invalid_stream_key", "stream is required")
		return
	}
	deleted, err := s.store.deleteRetentionPolicy(request.Context(), connection.config.ID, streamKey)
	if err != nil {
		writeError(writer, http.StatusInternalServerError, "retention_policy_delete_failed", err.Error())
		return
	}
	if !deleted {
		writeError(writer, http.StatusNotFound, "retention_policy_not_found", "retention policy not found")
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"deleted": true})
}
