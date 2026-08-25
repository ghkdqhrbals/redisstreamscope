package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	consumerStalledAfter       = 60 * time.Second
	consumerHistoryInterval    = 5 * time.Minute
	consumerHistoryRetention   = 7 * 24 * time.Hour
	topologyHistoryInterval    = time.Minute
	topologyHistoryRetention   = 30 * 24 * time.Hour
	monitoringEventRetention   = 30 * 24 * time.Hour
	consumerHistoryRowLimit    = 500_000
	consumerGroupHistoryLimit  = 250_000
	topologyHistoryRowLimit    = 50_000
	monitoringEventRowLimit    = 100_000
	monitoringHistoryMaxWindow = 30 * 24 * time.Hour
)

type monitoringHistoryState struct {
	mu              sync.Mutex
	schemaReady     bool
	lastMaintenance time.Time
}

var monitoringHistoryStates sync.Map // map[*store]*monitoringHistoryState

func historyStateFor(dataStore *store) *monitoringHistoryState {
	value, _ := monitoringHistoryStates.LoadOrStore(dataStore, &monitoringHistoryState{})
	return value.(*monitoringHistoryState)
}

func (s *store) ensureMonitoringHistorySchema(ctx context.Context) error {
	state := historyStateFor(s)
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.schemaReady {
		return nil
	}
	statements := []string{
		`CREATE TABLE IF NOT EXISTS consumer_group_current (
				connection_id TEXT NOT NULL,
				stream_key TEXT NOT NULL,
				group_name TEXT NOT NULL,
				observed_at TEXT NOT NULL,
				consumer_count INTEGER NOT NULL,
				pending INTEGER NOT NULL,
				lag INTEGER,
				last_delivered_id TEXT NOT NULL,
				consumer_sample_status TEXT NOT NULL,
				stalled_consumers INTEGER NOT NULL,
				PRIMARY KEY(connection_id, stream_key, group_name)
			)`,
		`CREATE TABLE IF NOT EXISTS consumer_current (
				connection_id TEXT NOT NULL,
				stream_key TEXT NOT NULL,
				group_name TEXT NOT NULL,
				consumer_name TEXT NOT NULL,
				observed_at TEXT NOT NULL,
				pending INTEGER NOT NULL,
				idle_ms INTEGER NOT NULL,
				inactive_ms INTEGER NOT NULL,
				stalled INTEGER NOT NULL,
				PRIMARY KEY(connection_id, stream_key, group_name, consumer_name)
			)`,
		`CREATE INDEX IF NOT EXISTS consumer_current_scope_idx
				ON consumer_current(connection_id, stream_key, group_name, observed_at DESC)`,
		`CREATE TABLE IF NOT EXISTS consumer_group_history_samples (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				recorded_at TEXT NOT NULL,
				connection_id TEXT NOT NULL,
				stream_key TEXT NOT NULL,
				group_name TEXT NOT NULL,
				consumer_count INTEGER NOT NULL,
				pending INTEGER NOT NULL,
				lag INTEGER,
				stalled_consumers INTEGER NOT NULL,
				UNIQUE(connection_id, stream_key, group_name, recorded_at)
			)`,
		`CREATE INDEX IF NOT EXISTS consumer_group_history_scope_time_idx
				ON consumer_group_history_samples(connection_id, stream_key, group_name, recorded_at DESC)`,
		`CREATE TABLE IF NOT EXISTS consumer_history_samples (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				recorded_at TEXT NOT NULL,
				connection_id TEXT NOT NULL,
				stream_key TEXT NOT NULL,
				group_name TEXT NOT NULL,
				consumer_name TEXT NOT NULL,
				pending INTEGER NOT NULL,
				idle_ms INTEGER NOT NULL,
				inactive_ms INTEGER NOT NULL,
				stalled INTEGER NOT NULL,
				UNIQUE(connection_id, stream_key, group_name, consumer_name, recorded_at)
			)`,
		`CREATE INDEX IF NOT EXISTS consumer_history_scope_time_idx
				ON consumer_history_samples(connection_id, stream_key, group_name, recorded_at DESC)`,
		`CREATE TABLE IF NOT EXISTS redis_topology_current (
				connection_id TEXT PRIMARY KEY,
				recorded_at TEXT NOT NULL,
				mode TEXT NOT NULL,
				up INTEGER NOT NULL,
				cluster_json TEXT NOT NULL,
				nodes_json TEXT NOT NULL
			)`,
		`CREATE TABLE IF NOT EXISTS redis_topology_history (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				recorded_at TEXT NOT NULL,
				connection_id TEXT NOT NULL,
				mode TEXT NOT NULL,
				up INTEGER NOT NULL,
				cluster_json TEXT NOT NULL,
				nodes_json TEXT NOT NULL,
				UNIQUE(connection_id, recorded_at)
			)`,
		`CREATE INDEX IF NOT EXISTS redis_topology_history_scope_time_idx
				ON redis_topology_history(connection_id, recorded_at DESC)`,
		`CREATE TABLE IF NOT EXISTS monitoring_events (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				occurred_at TEXT NOT NULL,
				connection_id TEXT NOT NULL,
				category TEXT NOT NULL,
				stream_key TEXT NOT NULL DEFAULT '',
				group_name TEXT NOT NULL DEFAULT '',
				consumer_name TEXT NOT NULL DEFAULT '',
				node_id TEXT NOT NULL DEFAULT '',
				event_type TEXT NOT NULL,
				from_value TEXT NOT NULL DEFAULT '',
				to_value TEXT NOT NULL DEFAULT '',
				details_json TEXT NOT NULL DEFAULT '{}'
			)`,
		`CREATE INDEX IF NOT EXISTS monitoring_events_scope_time_idx
				ON monitoring_events(connection_id, category, occurred_at DESC, id DESC)`,
	}
	for _, statement := range statements {
		if _, err := s.db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("monitoring history schema: %w", err)
		}
	}
	state.schemaReady = true
	return nil
}

type consumerHistoryEvent struct {
	ID           int64          `json:"id"`
	OccurredAt   time.Time      `json:"occurredAt"`
	Category     string         `json:"category"`
	ConnectionID string         `json:"connectionId"`
	StreamKey    string         `json:"streamKey,omitempty"`
	GroupName    string         `json:"groupName,omitempty"`
	ConsumerName string         `json:"consumerName,omitempty"`
	NodeID       string         `json:"nodeId,omitempty"`
	Type         string         `json:"type"`
	From         string         `json:"from,omitempty"`
	To           string         `json:"to,omitempty"`
	Details      map[string]any `json:"details"`
}

type consumerStateView struct {
	Name       string    `json:"name"`
	Pending    int64     `json:"pending"`
	IdleMs     int64     `json:"idleMs"`
	InactiveMs int64     `json:"inactiveMs"`
	Stalled    bool      `json:"stalled"`
	ObservedAt time.Time `json:"observedAt"`
}

type consumerGroupStateView struct {
	StreamKey            string              `json:"streamKey"`
	GroupName            string              `json:"groupName"`
	ConsumerCount        int64               `json:"consumerCount"`
	Pending              int64               `json:"pending"`
	Lag                  *int64              `json:"lag"`
	LastDeliveredID      string              `json:"lastDeliveredId"`
	StalledConsumers     int64               `json:"stalledConsumers"`
	ConsumerSampleStatus string              `json:"consumerSampleStatus"`
	ObservedAt           time.Time           `json:"observedAt"`
	Consumers            []consumerStateView `json:"consumers"`
}

type consumerGroupHistorySample struct {
	ID               int64     `json:"id"`
	RecordedAt       time.Time `json:"recordedAt"`
	StreamKey        string    `json:"streamKey"`
	GroupName        string    `json:"groupName"`
	ConsumerCount    int64     `json:"consumerCount"`
	Pending          int64     `json:"pending"`
	Lag              *int64    `json:"lag"`
	StalledConsumers int64     `json:"stalledConsumers"`
}

type consumerHistoryResponse struct {
	ConnectionID   string                       `json:"connectionId"`
	From           time.Time                    `json:"from"`
	To             time.Time                    `json:"to"`
	StalledAfterMs int64                        `json:"stalledAfterMs"`
	Current        []consumerGroupStateView     `json:"current"`
	Samples        []consumerGroupHistorySample `json:"samples"`
	Events         []consumerHistoryEvent       `json:"events"`
	NextCursor     int64                        `json:"nextCursor,omitempty"`
}

func consumerIsStalled(consumer consumerOperationalHealth) bool {
	if consumer.Pending <= 0 {
		return false
	}
	threshold := consumerStalledAfter.Milliseconds()
	return consumer.IdleMs >= threshold || consumer.InactiveMs >= threshold
}

func (s *store) recordConsumerHistory(ctx context.Context, snapshot operationalSnapshot) error {
	if snapshot.StreamSampledAt == nil || !snapshot.Up {
		return nil
	}
	if err := s.ensureMonitoringHistorySchema(ctx); err != nil {
		return err
	}
	recordedAt := snapshot.StreamSampledAt.UTC()
	recordedText := recordedAt.Format(time.RFC3339Nano)
	transaction, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer transaction.Rollback()

	var newest sql.NullString
	if err := transaction.QueryRowContext(ctx,
		`SELECT MAX(observed_at) FROM consumer_group_current WHERE connection_id=?`, snapshot.ConnectionID,
	).Scan(&newest); err != nil {
		return err
	}
	if newest.Valid && newest.String >= recordedText {
		return transaction.Commit()
	}

	observedStreams := make(map[string]struct{}, len(snapshot.Streams))
	for _, stream := range snapshot.Streams {
		observedStreams[stream.Key] = struct{}{}
		if !stream.Available {
			continue
		}
		previousGroups, err := loadCurrentGroupNames(ctx, transaction, snapshot.ConnectionID, stream.Key)
		if err != nil {
			return err
		}
		currentGroups := make(map[string]struct{}, len(stream.Groups))
		for _, group := range stream.Groups {
			currentGroups[group.Name] = struct{}{}
			_, groupExisted := previousGroups[group.Name]
			if !groupExisted {
				if err := insertMonitoringEvent(ctx, transaction, consumerHistoryEvent{
					OccurredAt: recordedAt, ConnectionID: snapshot.ConnectionID, Category: "consumer",
					StreamKey: stream.Key, GroupName: group.Name, Type: "group_joined",
				}); err != nil {
					return err
				}
			}

			stalledCount := int64(0)
			for _, consumer := range group.ConsumerStates {
				if consumerIsStalled(consumer) {
					stalledCount++
				}
			}
			if group.ConsumerSampleStatus != "sampled" && group.ConsumerSampleStatus != "truncated" {
				var existing int64
				_ = transaction.QueryRowContext(ctx, `SELECT stalled_consumers FROM consumer_group_current
					WHERE connection_id=? AND stream_key=? AND group_name=?`, snapshot.ConnectionID, stream.Key, group.Name).Scan(&existing)
				stalledCount = existing
			}
			if group.ConsumerSampleStatus == "sampled" {
				previousStalled := int64(0)
				if groupExisted {
					_ = transaction.QueryRowContext(ctx, `SELECT stalled_consumers FROM consumer_group_current
						WHERE connection_id=? AND stream_key=? AND group_name=?`, snapshot.ConnectionID, stream.Key, group.Name).Scan(&previousStalled)
				}
				if stalledCount > 0 && previousStalled == 0 {
					if err := insertMonitoringEvent(ctx, transaction, consumerHistoryEvent{
						OccurredAt: recordedAt, ConnectionID: snapshot.ConnectionID, Category: "consumer",
						StreamKey: stream.Key, GroupName: group.Name, Type: "group_stalled",
						Details: map[string]any{"stalledConsumers": stalledCount},
					}); err != nil {
						return err
					}
				} else if groupExisted && stalledCount == 0 && previousStalled > 0 {
					if err := insertMonitoringEvent(ctx, transaction, consumerHistoryEvent{
						OccurredAt: recordedAt, ConnectionID: snapshot.ConnectionID, Category: "consumer",
						StreamKey: stream.Key, GroupName: group.Name, Type: "group_recovered",
					}); err != nil {
						return err
					}
				}
			}
			if _, err := transaction.ExecContext(ctx, `INSERT INTO consumer_group_current(
				connection_id, stream_key, group_name, observed_at, consumer_count, pending, lag,
				last_delivered_id, consumer_sample_status, stalled_consumers
			) VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(connection_id, stream_key, group_name) DO UPDATE SET
				observed_at=excluded.observed_at, consumer_count=excluded.consumer_count,
				pending=excluded.pending, lag=excluded.lag, last_delivered_id=excluded.last_delivered_id,
				consumer_sample_status=excluded.consumer_sample_status, stalled_consumers=excluded.stalled_consumers`,
				snapshot.ConnectionID, stream.Key, group.Name, recordedText, group.Consumers, group.Pending,
				nullableInt64(group.Lag), group.LastDeliveredID, group.ConsumerSampleStatus, stalledCount,
			); err != nil {
				return err
			}

			if group.ConsumerSampleStatus == "sampled" {
				if err := recordCompleteConsumerSet(ctx, transaction, snapshot.ConnectionID, stream.Key, group, recordedAt); err != nil {
					return err
				}
			} else if group.ConsumerSampleStatus == "truncated" {
				if err := recordPartialConsumerSet(ctx, transaction, snapshot.ConnectionID, stream.Key, group, recordedAt); err != nil {
					return err
				}
			}
		}

		// A truncated global/group result cannot prove absence. Suppress removals
		// rather than emitting false leave events during bounded collection.
		if snapshot.GroupsTruncated == 0 {
			for groupName := range previousGroups {
				if _, exists := currentGroups[groupName]; exists {
					continue
				}
				if err := insertMonitoringEvent(ctx, transaction, consumerHistoryEvent{
					OccurredAt: recordedAt, ConnectionID: snapshot.ConnectionID, Category: "consumer",
					StreamKey: stream.Key, GroupName: groupName, Type: "group_left",
				}); err != nil {
					return err
				}
				if _, err := transaction.ExecContext(ctx, `DELETE FROM consumer_current
					WHERE connection_id=? AND stream_key=? AND group_name=?`, snapshot.ConnectionID, stream.Key, groupName); err != nil {
					return err
				}
				if _, err := transaction.ExecContext(ctx, `DELETE FROM consumer_group_current
					WHERE connection_id=? AND stream_key=? AND group_name=?`, snapshot.ConnectionID, stream.Key, groupName); err != nil {
					return err
				}
			}
		}
	}
	if snapshot.StreamsTruncated == 0 {
		if err := pruneUnmonitoredConsumerState(ctx, transaction, snapshot.ConnectionID, observedStreams); err != nil {
			return err
		}
	}

	if shouldWriteConsumerHistorySample(ctx, transaction, snapshot.ConnectionID, recordedAt) {
		if err := snapshotCurrentConsumerHistory(ctx, transaction, snapshot.ConnectionID, recordedText); err != nil {
			return err
		}
	}
	return transaction.Commit()
}

func pruneUnmonitoredConsumerState(ctx context.Context, transaction *sql.Tx, connectionID string, observed map[string]struct{}) error {
	rows, err := transaction.QueryContext(ctx, `SELECT DISTINCT stream_key FROM consumer_group_current WHERE connection_id=?`, connectionID)
	if err != nil {
		return err
	}
	stale := make([]string, 0)
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			rows.Close()
			return err
		}
		if _, exists := observed[key]; !exists {
			stale = append(stale, key)
		}
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, key := range stale {
		if _, err := transaction.ExecContext(ctx, `DELETE FROM consumer_current WHERE connection_id=? AND stream_key=?`, connectionID, key); err != nil {
			return err
		}
		if _, err := transaction.ExecContext(ctx, `DELETE FROM consumer_group_current WHERE connection_id=? AND stream_key=?`, connectionID, key); err != nil {
			return err
		}
	}
	return nil
}

func loadCurrentGroupNames(ctx context.Context, transaction *sql.Tx, connectionID, streamKey string) (map[string]struct{}, error) {
	rows, err := transaction.QueryContext(ctx, `SELECT group_name FROM consumer_group_current
		WHERE connection_id=? AND stream_key=?`, connectionID, streamKey)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make(map[string]struct{})
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		result[name] = struct{}{}
	}
	return result, rows.Err()
}

type persistedConsumerState struct {
	Pending    int64
	IdleMs     int64
	InactiveMs int64
	Stalled    bool
}

func loadCurrentConsumers(ctx context.Context, transaction *sql.Tx, connectionID, streamKey, groupName string) (map[string]persistedConsumerState, error) {
	rows, err := transaction.QueryContext(ctx, `SELECT consumer_name, pending, idle_ms, inactive_ms, stalled
		FROM consumer_current WHERE connection_id=? AND stream_key=? AND group_name=?`, connectionID, streamKey, groupName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make(map[string]persistedConsumerState)
	for rows.Next() {
		var name string
		var item persistedConsumerState
		var stalled int
		if err := rows.Scan(&name, &item.Pending, &item.IdleMs, &item.InactiveMs, &stalled); err != nil {
			return nil, err
		}
		item.Stalled = stalled == 1
		result[name] = item
	}
	return result, rows.Err()
}

func recordCompleteConsumerSet(ctx context.Context, transaction *sql.Tx, connectionID, streamKey string, group consumerGroupOperationalHealth, recordedAt time.Time) error {
	previous, err := loadCurrentConsumers(ctx, transaction, connectionID, streamKey, group.Name)
	if err != nil {
		return err
	}
	current := make(map[string]struct{}, len(group.ConsumerStates))
	for _, consumer := range group.ConsumerStates {
		current[consumer.Name] = struct{}{}
		if err := upsertConsumerAndTransitions(ctx, transaction, connectionID, streamKey, group.Name, consumer, previous, recordedAt); err != nil {
			return err
		}
	}
	for name, old := range previous {
		if _, exists := current[name]; exists {
			continue
		}
		if err := insertMonitoringEvent(ctx, transaction, consumerHistoryEvent{
			OccurredAt: recordedAt, ConnectionID: connectionID, Category: "consumer", StreamKey: streamKey,
			GroupName: group.Name, ConsumerName: name, Type: "consumer_left",
			Details: map[string]any{"pending": old.Pending, "stalled": old.Stalled},
		}); err != nil {
			return err
		}
		if _, err := transaction.ExecContext(ctx, `DELETE FROM consumer_current WHERE connection_id=? AND stream_key=? AND group_name=? AND consumer_name=?`,
			connectionID, streamKey, group.Name, name); err != nil {
			return err
		}
	}
	return nil
}

func recordPartialConsumerSet(ctx context.Context, transaction *sql.Tx, connectionID, streamKey string, group consumerGroupOperationalHealth, recordedAt time.Time) error {
	previous, err := loadCurrentConsumers(ctx, transaction, connectionID, streamKey, group.Name)
	if err != nil {
		return err
	}
	for _, consumer := range group.ConsumerStates {
		if err := upsertConsumerAndTransitions(ctx, transaction, connectionID, streamKey, group.Name, consumer, previous, recordedAt); err != nil {
			return err
		}
	}
	// Partial XINFO results cannot prove a leave, but retaining every consumer
	// ever observed would grow without bound under high churn. Keep only the
	// most recently observed bounded set without emitting leave events.
	_, err = transaction.ExecContext(ctx, `DELETE FROM consumer_current
		WHERE connection_id=? AND stream_key=? AND group_name=? AND consumer_name NOT IN (
			SELECT consumer_name FROM consumer_current
			WHERE connection_id=? AND stream_key=? AND group_name=?
			ORDER BY observed_at DESC, consumer_name LIMIT ?
		)`, connectionID, streamKey, group.Name, connectionID, streamKey, group.Name, operationalMaxConsumersPerGroup)
	return err
}

func upsertConsumerAndTransitions(ctx context.Context, transaction *sql.Tx, connectionID, streamKey, groupName string, consumer consumerOperationalHealth, previous map[string]persistedConsumerState, recordedAt time.Time) error {
	old, exists := previous[consumer.Name]
	stalled := consumerIsStalled(consumer)
	if !exists {
		if err := insertMonitoringEvent(ctx, transaction, consumerHistoryEvent{
			OccurredAt: recordedAt, ConnectionID: connectionID, Category: "consumer", StreamKey: streamKey,
			GroupName: groupName, ConsumerName: consumer.Name, Type: "consumer_joined",
			Details: map[string]any{"pending": consumer.Pending},
		}); err != nil {
			return err
		}
	}
	if stalled && (!exists || !old.Stalled) {
		if err := insertMonitoringEvent(ctx, transaction, consumerHistoryEvent{
			OccurredAt: recordedAt, ConnectionID: connectionID, Category: "consumer", StreamKey: streamKey,
			GroupName: groupName, ConsumerName: consumer.Name, Type: "consumer_stalled",
			Details: map[string]any{"pending": consumer.Pending, "idleMs": consumer.IdleMs, "inactiveMs": consumer.InactiveMs},
		}); err != nil {
			return err
		}
	} else if exists && old.Stalled && !stalled {
		if err := insertMonitoringEvent(ctx, transaction, consumerHistoryEvent{
			OccurredAt: recordedAt, ConnectionID: connectionID, Category: "consumer", StreamKey: streamKey,
			GroupName: groupName, ConsumerName: consumer.Name, Type: "consumer_recovered",
			Details: map[string]any{"pending": consumer.Pending, "idleMs": consumer.IdleMs, "inactiveMs": consumer.InactiveMs},
		}); err != nil {
			return err
		}
	}
	_, err := transaction.ExecContext(ctx, `INSERT INTO consumer_current(
		connection_id, stream_key, group_name, consumer_name, observed_at, pending, idle_ms, inactive_ms, stalled
	) VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(connection_id, stream_key, group_name, consumer_name) DO UPDATE SET
		observed_at=excluded.observed_at, pending=excluded.pending, idle_ms=excluded.idle_ms,
		inactive_ms=excluded.inactive_ms, stalled=excluded.stalled`, connectionID, streamKey, groupName,
		consumer.Name, recordedAt.UTC().Format(time.RFC3339Nano), consumer.Pending, consumer.IdleMs,
		consumer.InactiveMs, boolToInt(stalled))
	return err
}

func shouldWriteConsumerHistorySample(ctx context.Context, transaction *sql.Tx, connectionID string, recordedAt time.Time) bool {
	var raw sql.NullString
	if err := transaction.QueryRowContext(ctx, `SELECT MAX(recorded_at) FROM consumer_group_history_samples WHERE connection_id=?`, connectionID).Scan(&raw); err != nil || !raw.Valid {
		return true
	}
	previous, err := time.Parse(time.RFC3339Nano, raw.String)
	return err != nil || recordedAt.Sub(previous) >= consumerHistoryInterval
}

func snapshotCurrentConsumerHistory(ctx context.Context, transaction *sql.Tx, connectionID, recordedAt string) error {
	if _, err := transaction.ExecContext(ctx, `INSERT OR IGNORE INTO consumer_group_history_samples(
		recorded_at, connection_id, stream_key, group_name, consumer_count, pending, lag, stalled_consumers
	) SELECT ?, connection_id, stream_key, group_name, consumer_count, pending, lag, stalled_consumers
	  FROM consumer_group_current WHERE connection_id=?`, recordedAt, connectionID); err != nil {
		return err
	}
	_, err := transaction.ExecContext(ctx, `INSERT OR IGNORE INTO consumer_history_samples(
		recorded_at, connection_id, stream_key, group_name, consumer_name, pending, idle_ms, inactive_ms, stalled
	) SELECT ?, connection_id, stream_key, group_name, consumer_name, pending, idle_ms, inactive_ms, stalled
	  FROM consumer_current WHERE connection_id=?`, recordedAt, connectionID)
	return err
}

func insertMonitoringEvent(ctx context.Context, transaction *sql.Tx, event consumerHistoryEvent) error {
	details := event.Details
	if details == nil {
		details = map[string]any{}
	}
	raw, err := json.Marshal(details)
	if err != nil {
		return err
	}
	_, err = transaction.ExecContext(ctx, `INSERT INTO monitoring_events(
		occurred_at, connection_id, category, stream_key, group_name, consumer_name,
		node_id, event_type, from_value, to_value, details_json
	) VALUES(?,?,?,?,?,?,?,?,?,?,?)`, event.OccurredAt.UTC().Format(time.RFC3339Nano), event.ConnectionID,
		event.Category, event.StreamKey, event.GroupName, event.ConsumerName, event.NodeID,
		event.Type, event.From, event.To, string(raw))
	return err
}

type topologyModel struct {
	ConnectionID string             `json:"connectionId"`
	RecordedAt   time.Time          `json:"recordedAt"`
	Mode         string             `json:"mode"`
	Up           bool               `json:"up"`
	Cluster      redisClusterHealth `json:"cluster"`
	Nodes        []redisNodeHealth  `json:"nodes"`
}

func (s *store) recordTopologyHistory(ctx context.Context, snapshot operationalSnapshot) error {
	if snapshot.NodeSampledAt == nil {
		return nil
	}
	if err := s.ensureMonitoringHistorySchema(ctx); err != nil {
		return err
	}
	recordedAt := snapshot.NodeSampledAt.UTC()
	recordedText := recordedAt.Format(time.RFC3339Nano)
	transaction, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer transaction.Rollback()

	previous, exists, err := loadCurrentTopology(ctx, transaction, snapshot.ConnectionID)
	if err != nil {
		return err
	}
	if exists && !recordedAt.After(previous.RecordedAt) {
		return transaction.Commit()
	}
	current := topologyModel{
		ConnectionID: snapshot.ConnectionID, RecordedAt: recordedAt, Mode: snapshot.Mode,
		Up: snapshot.Up, Cluster: snapshot.Cluster, Nodes: append([]redisNodeHealth{}, snapshot.Nodes...),
	}
	if exists && !current.Up && len(current.Nodes) == 0 {
		// A failed PING/INFO means node membership is unknown, not that every
		// node left the cluster. Preserve identities and record them as down so
		// recovery does not create a false leave/join sequence.
		current.Cluster = previous.Cluster
		current.Cluster.Message = snapshot.Error
		current.Nodes = append([]redisNodeHealth{}, previous.Nodes...)
		for index := range current.Nodes {
			current.Nodes[index].Up = false
			current.Nodes[index].Error = snapshot.Error
		}
	}
	if exists {
		if err := deriveTopologyEvents(ctx, transaction, previous, current); err != nil {
			return err
		}
	}
	clusterJSON, err := json.Marshal(current.Cluster)
	if err != nil {
		return err
	}
	nodesJSON, err := json.Marshal(current.Nodes)
	if err != nil {
		return err
	}
	if _, err := transaction.ExecContext(ctx, `INSERT INTO redis_topology_current(
		connection_id, recorded_at, mode, up, cluster_json, nodes_json
	) VALUES(?,?,?,?,?,?) ON CONFLICT(connection_id) DO UPDATE SET recorded_at=excluded.recorded_at,
		mode=excluded.mode, up=excluded.up, cluster_json=excluded.cluster_json, nodes_json=excluded.nodes_json`,
		current.ConnectionID, recordedText, current.Mode, boolToInt(current.Up), string(clusterJSON), string(nodesJSON)); err != nil {
		return err
	}
	writeHistory := !exists
	if exists {
		var latestHistory sql.NullString
		if err := transaction.QueryRowContext(ctx, `SELECT MAX(recorded_at) FROM redis_topology_history WHERE connection_id=?`, snapshot.ConnectionID).Scan(&latestHistory); err != nil {
			return err
		}
		if !latestHistory.Valid {
			writeHistory = true
		} else if last, parseErr := time.Parse(time.RFC3339Nano, latestHistory.String); parseErr != nil || recordedAt.Sub(last) >= topologyHistoryInterval {
			writeHistory = true
		}
	}
	if writeHistory {
		if _, err := transaction.ExecContext(ctx, `INSERT OR IGNORE INTO redis_topology_history(
			recorded_at, connection_id, mode, up, cluster_json, nodes_json
		) VALUES(?,?,?,?,?,?)`, recordedText, current.ConnectionID, current.Mode, boolToInt(current.Up), string(clusterJSON), string(nodesJSON)); err != nil {
			return err
		}
	}
	return transaction.Commit()
}

type topologyQueryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func loadCurrentTopology(ctx context.Context, queryer topologyQueryer, connectionID string) (topologyModel, bool, error) {
	var result topologyModel
	var recordedAt, clusterJSON, nodesJSON string
	var up int
	err := queryer.QueryRowContext(ctx, `SELECT recorded_at, mode, up, cluster_json, nodes_json
		FROM redis_topology_current WHERE connection_id=?`, connectionID).Scan(&recordedAt, &result.Mode, &up, &clusterJSON, &nodesJSON)
	if errors.Is(err, sql.ErrNoRows) {
		return topologyModel{}, false, nil
	}
	if err != nil {
		return topologyModel{}, false, err
	}
	result.ConnectionID = connectionID
	result.RecordedAt, _ = time.Parse(time.RFC3339Nano, recordedAt)
	result.Up = up == 1
	if err := json.Unmarshal([]byte(clusterJSON), &result.Cluster); err != nil {
		return topologyModel{}, false, err
	}
	if err := json.Unmarshal([]byte(nodesJSON), &result.Nodes); err != nil {
		return topologyModel{}, false, err
	}
	if result.Nodes == nil {
		result.Nodes = []redisNodeHealth{}
	}
	return result, true, nil
}

func topologyNodeKey(node redisNodeHealth) string {
	if node.Address != "" {
		return node.Address
	}
	return node.ID
}

func deriveTopologyEvents(ctx context.Context, transaction *sql.Tx, previous, current topologyModel) error {
	if previous.Up != current.Up {
		eventType := "connection_down"
		if current.Up {
			eventType = "connection_up"
		}
		if err := insertMonitoringEvent(ctx, transaction, consumerHistoryEvent{
			OccurredAt: current.RecordedAt, ConnectionID: current.ConnectionID, Category: "topology",
			Type: eventType, From: strconv.FormatBool(previous.Up), To: strconv.FormatBool(current.Up),
		}); err != nil {
			return err
		}
	}
	if previous.Cluster.State != current.Cluster.State {
		if err := insertMonitoringEvent(ctx, transaction, consumerHistoryEvent{
			OccurredAt: current.RecordedAt, ConnectionID: current.ConnectionID, Category: "topology",
			Type: "cluster_state_changed", From: previous.Cluster.State, To: current.Cluster.State,
		}); err != nil {
			return err
		}
	}
	if previous.Cluster.SlotsFail != current.Cluster.SlotsFail || previous.Cluster.SlotsPFail != current.Cluster.SlotsPFail {
		if err := insertMonitoringEvent(ctx, transaction, consumerHistoryEvent{
			OccurredAt: current.RecordedAt, ConnectionID: current.ConnectionID, Category: "topology",
			Type: "cluster_slot_health_changed",
			Details: map[string]any{"previousFail": previous.Cluster.SlotsFail, "fail": current.Cluster.SlotsFail,
				"previousPFail": previous.Cluster.SlotsPFail, "pfail": current.Cluster.SlotsPFail},
		}); err != nil {
			return err
		}
	}
	previousNodes := make(map[string]redisNodeHealth, len(previous.Nodes))
	currentNodes := make(map[string]redisNodeHealth, len(current.Nodes))
	for _, node := range previous.Nodes {
		previousNodes[topologyNodeKey(node)] = node
	}
	for _, node := range current.Nodes {
		key := topologyNodeKey(node)
		currentNodes[key] = node
		old, exists := previousNodes[key]
		if !exists {
			if err := insertTopologyNodeEvent(ctx, transaction, current, node, "node_joined", "", node.Role, nil); err != nil {
				return err
			}
			continue
		}
		transitions := []struct {
			changed                       bool
			category, eventType, from, to string
			details                       map[string]any
		}{
			{old.ID != node.ID, "topology", "node_restarted", old.ID, node.ID, nil},
			{old.Up != node.Up, "topology", map[bool]string{true: "node_up", false: "node_down"}[node.Up], strconv.FormatBool(old.Up), strconv.FormatBool(node.Up), nil},
			{old.Role != node.Role, "topology", "node_role_changed", old.Role, node.Role, nil},
			{old.MasterLinkStatus != node.MasterLinkStatus, "replication", "master_link_changed", old.MasterLinkStatus, node.MasterLinkStatus, nil},
			{old.RDBLastSaveStatus != node.RDBLastSaveStatus, "persistence", "rdb_status_changed", old.RDBLastSaveStatus, node.RDBLastSaveStatus, nil},
			{old.AOFLastRewriteStatus != node.AOFLastRewriteStatus, "persistence", "aof_status_changed", old.AOFLastRewriteStatus, node.AOFLastRewriteStatus, nil},
			{old.Loading != node.Loading, "persistence", "loading_state_changed", strconv.FormatBool(old.Loading), strconv.FormatBool(node.Loading), nil},
		}
		for _, transition := range transitions {
			if !transition.changed {
				continue
			}
			eventType := transition.eventType
			if eventType == "node_role_changed" && (old.Role == "slave" || old.Role == "replica") && node.Role == "master" {
				eventType = "failover_promoted"
			}
			if err := insertTopologyNodeEventWithCategory(ctx, transaction, current, node, transition.category, eventType, transition.from, transition.to, transition.details); err != nil {
				return err
			}
		}
	}
	for key, node := range previousNodes {
		if _, exists := currentNodes[key]; exists {
			continue
		}
		if err := insertTopologyNodeEvent(ctx, transaction, current, node, "node_left", node.Role, "", nil); err != nil {
			return err
		}
	}
	return nil
}

func insertTopologyNodeEvent(ctx context.Context, transaction *sql.Tx, current topologyModel, node redisNodeHealth, eventType, from, to string, details map[string]any) error {
	return insertTopologyNodeEventWithCategory(ctx, transaction, current, node, "topology", eventType, from, to, details)
}

func insertTopologyNodeEventWithCategory(ctx context.Context, transaction *sql.Tx, current topologyModel, node redisNodeHealth, category, eventType, from, to string, details map[string]any) error {
	if details == nil {
		details = map[string]any{}
	}
	details["address"] = node.Address
	details["nodeRunId"] = node.ID
	return insertMonitoringEvent(ctx, transaction, consumerHistoryEvent{
		OccurredAt: current.RecordedAt, ConnectionID: current.ConnectionID, Category: category,
		NodeID: topologyNodeKey(node), Type: eventType, From: from, To: to, Details: details,
	})
}

func (s *store) recordOperationalSnapshot(ctx context.Context, snapshot operationalSnapshot) error {
	if err := s.recordConsumerHistory(ctx, snapshot); err != nil {
		return err
	}
	if err := s.recordTopologyHistory(ctx, snapshot); err != nil {
		return err
	}
	if err := s.recordCapacitySample(ctx, snapshot); err != nil {
		return err
	}
	state := historyStateFor(s)
	state.mu.Lock()
	due := state.lastMaintenance.IsZero() || snapshot.CollectedAt.Sub(state.lastMaintenance) >= time.Hour
	if due {
		state.lastMaintenance = snapshot.CollectedAt
	}
	state.mu.Unlock()
	if due {
		return s.maintainMonitoringHistory(ctx, snapshot.CollectedAt)
	}
	return nil
}

func (s *store) maintainMonitoringHistory(ctx context.Context, now time.Time) error {
	if err := s.ensureMonitoringHistorySchema(ctx); err != nil {
		return err
	}
	now = now.UTC()
	transaction, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer transaction.Rollback()
	deletions := []struct {
		query string
		args  []any
	}{
		{`DELETE FROM consumer_group_current WHERE observed_at<?`, []any{now.Add(-consumerHistoryRetention).Format(time.RFC3339Nano)}},
		{`DELETE FROM consumer_current WHERE observed_at<?`, []any{now.Add(-consumerHistoryRetention).Format(time.RFC3339Nano)}},
		{`DELETE FROM consumer_group_history_samples WHERE recorded_at<?`, []any{now.Add(-consumerHistoryRetention).Format(time.RFC3339Nano)}},
		{`DELETE FROM consumer_history_samples WHERE recorded_at<?`, []any{now.Add(-consumerHistoryRetention).Format(time.RFC3339Nano)}},
		{`DELETE FROM redis_topology_current WHERE recorded_at<?`, []any{now.Add(-topologyHistoryRetention).Format(time.RFC3339Nano)}},
		{`DELETE FROM redis_topology_history WHERE recorded_at<?`, []any{now.Add(-topologyHistoryRetention).Format(time.RFC3339Nano)}},
		{`DELETE FROM monitoring_events WHERE occurred_at<?`, []any{now.Add(-monitoringEventRetention).Format(time.RFC3339Nano)}},
	}
	for _, deletion := range deletions {
		if _, err := transaction.ExecContext(ctx, deletion.query, deletion.args...); err != nil {
			return err
		}
	}
	for table, limit := range map[string]int{
		"consumer_group_history_samples": consumerGroupHistoryLimit,
		"consumer_history_samples":       consumerHistoryRowLimit,
		"redis_topology_history":         topologyHistoryRowLimit,
		"monitoring_events":              monitoringEventRowLimit,
	} {
		query := fmt.Sprintf(`DELETE FROM %s WHERE id IN (SELECT id FROM %s ORDER BY id DESC LIMIT -1 OFFSET ?)`, table, table)
		if _, err := transaction.ExecContext(ctx, query, limit); err != nil {
			return err
		}
	}
	if err := maintainCapacityHistoryTx(ctx, transaction, now); err != nil {
		return err
	}
	return transaction.Commit()
}

func parseMonitoringTimeRange(values map[string][]string, now time.Time) (time.Time, time.Time, error) {
	to := now.UTC()
	from := to.Add(-24 * time.Hour)
	if raw := strings.TrimSpace(firstQueryValue(values, "to")); raw != "" {
		parsed, err := time.Parse(time.RFC3339, raw)
		if err != nil {
			return time.Time{}, time.Time{}, errors.New("to must be an RFC3339 timestamp")
		}
		to = parsed.UTC()
	}
	if raw := strings.TrimSpace(firstQueryValue(values, "from")); raw != "" {
		parsed, err := time.Parse(time.RFC3339, raw)
		if err != nil {
			return time.Time{}, time.Time{}, errors.New("from must be an RFC3339 timestamp")
		}
		from = parsed.UTC()
	}
	if !from.Before(to) {
		return time.Time{}, time.Time{}, errors.New("from must be before to")
	}
	if to.Sub(from) > monitoringHistoryMaxWindow {
		return time.Time{}, time.Time{}, fmt.Errorf("time range must not exceed %s", monitoringHistoryMaxWindow)
	}
	return from, to, nil
}

func firstQueryValue(values map[string][]string, key string) string {
	items := values[key]
	if len(items) == 0 {
		return ""
	}
	return items[0]
}

func (s *store) queryConsumerHistory(ctx context.Context, connectionID, streamKey, groupName string, from, to time.Time, cursor int64, limit int) (consumerHistoryResponse, error) {
	if err := s.ensureMonitoringHistorySchema(ctx); err != nil {
		return consumerHistoryResponse{}, err
	}
	result := consumerHistoryResponse{ConnectionID: connectionID, From: from, To: to, StalledAfterMs: consumerStalledAfter.Milliseconds(),
		Current: []consumerGroupStateView{}, Samples: []consumerGroupHistorySample{}, Events: []consumerHistoryEvent{}}

	currentQuery := `SELECT stream_key, group_name, observed_at, consumer_count, pending, lag,
		last_delivered_id, consumer_sample_status, stalled_consumers FROM consumer_group_current WHERE connection_id=?`
	args := []any{connectionID}
	if streamKey != "" {
		currentQuery += ` AND stream_key=?`
		args = append(args, streamKey)
	}
	if groupName != "" {
		currentQuery += ` AND group_name=?`
		args = append(args, groupName)
	}
	currentQuery += ` ORDER BY stream_key, group_name LIMIT 2048`
	rows, err := s.db.QueryContext(ctx, currentQuery, args...)
	if err != nil {
		return result, err
	}
	for rows.Next() {
		var item consumerGroupStateView
		var recordedAt string
		var lag sql.NullInt64
		if err := rows.Scan(&item.StreamKey, &item.GroupName, &recordedAt, &item.ConsumerCount, &item.Pending,
			&lag, &item.LastDeliveredID, &item.ConsumerSampleStatus, &item.StalledConsumers); err != nil {
			rows.Close()
			return result, err
		}
		item.ObservedAt, _ = time.Parse(time.RFC3339Nano, recordedAt)
		if lag.Valid {
			value := lag.Int64
			item.Lag = &value
		}
		item.Consumers, err = s.queryCurrentConsumers(ctx, connectionID, item.StreamKey, item.GroupName)
		if err != nil {
			rows.Close()
			return result, err
		}
		result.Current = append(result.Current, item)
	}
	if err := rows.Close(); err != nil {
		return result, err
	}

	sampleQuery := `SELECT id, recorded_at, stream_key, group_name, consumer_count, pending, lag, stalled_consumers
		FROM consumer_group_history_samples WHERE connection_id=? AND recorded_at>=? AND recorded_at<=?`
	sampleArgs := []any{connectionID, from.Format(time.RFC3339Nano), to.Format(time.RFC3339Nano)}
	if streamKey != "" {
		sampleQuery += ` AND stream_key=?`
		sampleArgs = append(sampleArgs, streamKey)
	}
	if groupName != "" {
		sampleQuery += ` AND group_name=?`
		sampleArgs = append(sampleArgs, groupName)
	}
	sampleQuery += ` ORDER BY recorded_at, id LIMIT 5000`
	sampleRows, err := s.db.QueryContext(ctx, sampleQuery, sampleArgs...)
	if err != nil {
		return result, err
	}
	for sampleRows.Next() {
		var item consumerGroupHistorySample
		var recordedAt string
		var lag sql.NullInt64
		if err := sampleRows.Scan(&item.ID, &recordedAt, &item.StreamKey, &item.GroupName, &item.ConsumerCount,
			&item.Pending, &lag, &item.StalledConsumers); err != nil {
			sampleRows.Close()
			return result, err
		}
		item.RecordedAt, _ = time.Parse(time.RFC3339Nano, recordedAt)
		if lag.Valid {
			value := lag.Int64
			item.Lag = &value
		}
		result.Samples = append(result.Samples, item)
	}
	if err := sampleRows.Close(); err != nil {
		return result, err
	}

	events, nextCursor, err := s.queryMonitoringEvents(ctx, connectionID, []string{"consumer"}, streamKey, groupName, from, to, cursor, limit)
	if err != nil {
		return result, err
	}
	result.Events = events
	result.NextCursor = nextCursor
	return result, nil
}

func (s *store) queryCurrentConsumers(ctx context.Context, connectionID, streamKey, groupName string) ([]consumerStateView, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT consumer_name, observed_at, pending, idle_ms, inactive_ms, stalled
		FROM consumer_current WHERE connection_id=? AND stream_key=? AND group_name=? ORDER BY consumer_name LIMIT ?`,
		connectionID, streamKey, groupName, operationalMaxConsumersPerGroup)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]consumerStateView, 0)
	for rows.Next() {
		var item consumerStateView
		var observedAt string
		var stalled int
		if err := rows.Scan(&item.Name, &observedAt, &item.Pending, &item.IdleMs, &item.InactiveMs, &stalled); err != nil {
			return nil, err
		}
		item.ObservedAt, _ = time.Parse(time.RFC3339Nano, observedAt)
		item.Stalled = stalled == 1
		result = append(result, item)
	}
	return result, rows.Err()
}

func (s *store) queryMonitoringEvents(ctx context.Context, connectionID string, categories []string, streamKey, groupName string, from, to time.Time, cursor int64, limit int) ([]consumerHistoryEvent, int64, error) {
	if limit <= 0 {
		limit = 100
	}
	if limit > 500 {
		limit = 500
	}
	query := `SELECT id, occurred_at, category, stream_key, group_name, consumer_name, node_id,
		event_type, from_value, to_value, details_json FROM monitoring_events
		WHERE connection_id=? AND occurred_at>=? AND occurred_at<=?`
	args := []any{connectionID, from.Format(time.RFC3339Nano), to.Format(time.RFC3339Nano)}
	if len(categories) > 0 {
		placeholders := make([]string, len(categories))
		for index, category := range categories {
			placeholders[index] = "?"
			args = append(args, category)
		}
		query += ` AND category IN (` + strings.Join(placeholders, ",") + `)`
	}
	if streamKey != "" {
		query += ` AND stream_key=?`
		args = append(args, streamKey)
	}
	if groupName != "" {
		query += ` AND group_name=?`
		args = append(args, groupName)
	}
	if cursor > 0 {
		query += ` AND id<?`
		args = append(args, cursor)
	}
	query += ` ORDER BY id DESC LIMIT ?`
	args = append(args, limit+1)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	result := make([]consumerHistoryEvent, 0, limit+1)
	for rows.Next() {
		var item consumerHistoryEvent
		var occurredAt, details string
		if err := rows.Scan(&item.ID, &occurredAt, &item.Category, &item.StreamKey, &item.GroupName,
			&item.ConsumerName, &item.NodeID, &item.Type, &item.From, &item.To, &details); err != nil {
			return nil, 0, err
		}
		item.ConnectionID = connectionID
		item.OccurredAt, _ = time.Parse(time.RFC3339Nano, occurredAt)
		item.Details = map[string]any{}
		_ = json.Unmarshal([]byte(details), &item.Details)
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	nextCursor := int64(0)
	if len(result) > limit {
		result = result[:limit]
		nextCursor = result[len(result)-1].ID
	}
	return result, nextCursor, nil
}

func (s *apiServer) consumerHistory(writer http.ResponseWriter, request *http.Request) {
	connection, err := s.redis.get(request.URL.Query().Get("connectionId"))
	if err != nil {
		writeError(writer, http.StatusBadRequest, "unknown_connection", err.Error())
		return
	}
	streamKey := strings.TrimSpace(request.URL.Query().Get("streamKey"))
	if streamKey == "" {
		streamKey = strings.TrimSpace(request.URL.Query().Get("stream"))
	}
	if !s.requireNonAdminStreamScope(writer, request, "groups:read", connection.config.ID, streamKey, "consumer_history_scope_required") {
		return
	}
	from, to, err := parseMonitoringTimeRange(request.URL.Query(), time.Now())
	if err != nil {
		writeError(writer, http.StatusBadRequest, "invalid_time_range", err.Error())
		return
	}
	cursor, err := strconv.ParseInt(firstNonEmpty(request.URL.Query().Get("cursor"), "0"), 10, 64)
	if err != nil || cursor < 0 {
		writeError(writer, http.StatusBadRequest, "invalid_cursor", "cursor must be a positive integer")
		return
	}
	limit := int(int64Query(request.URL.Query(), "limit", 100, 1, 500))
	result, err := s.store.queryConsumerHistory(request.Context(), connection.config.ID,
		streamKey, strings.TrimSpace(request.URL.Query().Get("group")),
		from, to, cursor, limit)
	if err != nil {
		writeError(writer, http.StatusInternalServerError, "consumer_history_failed", err.Error())
		return
	}
	writeJSON(writer, http.StatusOK, result)
}

type topologyEventsResponse struct {
	ConnectionID string                 `json:"connectionId"`
	From         time.Time              `json:"from"`
	To           time.Time              `json:"to"`
	Items        []consumerHistoryEvent `json:"items"`
	NextCursor   int64                  `json:"nextCursor,omitempty"`
}

func (s *apiServer) topology(writer http.ResponseWriter, request *http.Request) {
	connection, err := s.redis.get(request.URL.Query().Get("connectionId"))
	if err != nil {
		writeError(writer, http.StatusBadRequest, "unknown_connection", err.Error())
		return
	}
	if err := s.store.ensureMonitoringHistorySchema(request.Context()); err != nil {
		writeError(writer, http.StatusInternalServerError, "topology_failed", err.Error())
		return
	}
	model, exists, err := loadCurrentTopology(request.Context(), s.store.db, connection.config.ID)
	if err != nil {
		writeError(writer, http.StatusInternalServerError, "topology_failed", err.Error())
		return
	}
	if !exists {
		if snapshot, ok := s.operationalMonitor().Snapshot(connection.config.ID); ok {
			_ = s.store.recordTopologyHistory(request.Context(), snapshot)
			model, exists, err = loadCurrentTopology(request.Context(), s.store.db, connection.config.ID)
		}
	}
	if err != nil {
		writeError(writer, http.StatusInternalServerError, "topology_failed", err.Error())
		return
	}
	if !exists {
		writeError(writer, http.StatusNotFound, "topology_not_sampled", "topology has not been sampled yet")
		return
	}
	writeJSON(writer, http.StatusOK, model)
}

func (s *apiServer) topologyEvents(writer http.ResponseWriter, request *http.Request) {
	connection, err := s.redis.get(request.URL.Query().Get("connectionId"))
	if err != nil {
		writeError(writer, http.StatusBadRequest, "unknown_connection", err.Error())
		return
	}
	from, to, err := parseMonitoringTimeRange(request.URL.Query(), time.Now())
	if err != nil {
		writeError(writer, http.StatusBadRequest, "invalid_time_range", err.Error())
		return
	}
	cursor, err := strconv.ParseInt(firstNonEmpty(request.URL.Query().Get("cursor"), "0"), 10, 64)
	if err != nil || cursor < 0 {
		writeError(writer, http.StatusBadRequest, "invalid_cursor", "cursor must be a positive integer")
		return
	}
	limit := int(int64Query(request.URL.Query(), "limit", 100, 1, 500))
	items, nextCursor, err := s.store.queryMonitoringEvents(request.Context(), connection.config.ID,
		[]string{"topology", "replication", "persistence"}, "", "", from, to, cursor, limit)
	if err != nil {
		writeError(writer, http.StatusInternalServerError, "topology_events_failed", err.Error())
		return
	}
	writeJSON(writer, http.StatusOK, topologyEventsResponse{ConnectionID: connection.config.ID, From: from, To: to, Items: items, NextCursor: nextCursor})
}
