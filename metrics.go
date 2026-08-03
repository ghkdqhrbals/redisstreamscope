package main

import (
	"context"
	"database/sql"
	"errors"
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
	metricCollectionInterval = time.Second
	metricRawRetention       = 62 * time.Minute
	metricRetention          = 7 * 24 * time.Hour
	metricMaxPoints          = 600
)

type streamMetricSample struct {
	RecordedAt      time.Time
	ConnectionID    string
	StreamKey       string
	Entries         int64
	ConsumerGroups  int
	ConsumerCount   int64
	TotalLag        int64
	LagKnown        bool
	Pending         int64
	LastDeliveredID string
	ConsumeDelayMs  *int64
	RedisLatencyMs  float64
	PublishedTotal  int64
	ConsumedTotal   int64
	PublishRate     *float64
	ConsumeRate     *float64
	LagDelta        *float64
}

type streamMetricState struct {
	RecordedAt      time.Time
	LastDeliveredID string
	ConsumeDelayMs  *int64
	PublishedTotal  int64
	ConsumedTotal   int64
	TotalLag        int64
	LagKnown        bool
}

type consumerGroupMetricSample struct {
	RecordedAt      time.Time
	ConnectionID    string
	StreamKey       string
	GroupName       string
	ConsumerCount   int64
	Pending         int64
	Lag             int64
	LagKnown        bool
	LastDeliveredID string
	ConsumeDelayMs  *int64
	ConsumedTotal   int64
	ConsumeRate     *float64
	LagDelta        *float64
}

type consumerGroupMetricState struct {
	RecordedAt      time.Time
	LastDeliveredID string
	ConsumeDelayMs  *int64
	ConsumedTotal   int64
	Lag             int64
	LagKnown        bool
}

type consumerGroupMetricValue struct {
	ConsumerCount  float64  `json:"consumerCount"`
	Pending        float64  `json:"pending"`
	Lag            *float64 `json:"lag"`
	ConsumeDelayMs *float64 `json:"consumeDelayMs"`
	ConsumeRate    *float64 `json:"consumeRate"`
	LagDelta       *float64 `json:"lagDelta"`
}

type consumerGroupMetricPoint struct {
	Timestamp time.Time                           `json:"timestamp"`
	Values    map[string]consumerGroupMetricValue `json:"values"`
}

type streamMetricPoint struct {
	Timestamp      time.Time `json:"timestamp"`
	Entries        float64   `json:"entries"`
	ConsumerGroups float64   `json:"consumerGroups"`
	ConsumerCount  float64   `json:"consumerCount"`
	TotalLag       *float64  `json:"totalLag"`
	Pending        float64   `json:"pending"`
	ConsumeDelayMs *float64  `json:"consumeDelayMs"`
	RedisLatencyMs float64   `json:"redisLatencyMs"`
	PublishRate    *float64  `json:"publishRate"`
	ConsumeRate    *float64  `json:"consumeRate"`
	LagDelta       *float64  `json:"lagDelta"`
}

func (s *store) writeMetricSamples(ctx context.Context, samples []streamMetricSample) error {
	if len(samples) == 0 {
		return nil
	}
	transaction, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer transaction.Rollback()
	statement, err := transaction.PrepareContext(ctx, `
		INSERT INTO stream_metric_samples(
			recorded_at, connection_id, stream_key, entries, consumer_groups,
			consumer_count, total_lag, lag_known, pending, last_delivered_id,
			consume_delay_ms, redis_latency_ms, published_total, consumed_total,
			publish_rate, consume_rate, lag_delta
		) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
	`)
	if err != nil {
		return err
	}
	defer statement.Close()
	for _, sample := range samples {
		if _, err := statement.ExecContext(
			ctx,
			sample.RecordedAt.UTC().Format(time.RFC3339Nano),
			sample.ConnectionID,
			sample.StreamKey,
			sample.Entries,
			sample.ConsumerGroups,
			sample.ConsumerCount,
			sample.TotalLag,
			boolToInt(sample.LagKnown),
			sample.Pending,
			sample.LastDeliveredID,
			nullableInt64(sample.ConsumeDelayMs),
			sample.RedisLatencyMs,
			sample.PublishedTotal,
			sample.ConsumedTotal,
			nullableFloat64(sample.PublishRate),
			nullableFloat64(sample.ConsumeRate),
			nullableFloat64(sample.LagDelta),
		); err != nil {
			return err
		}
	}
	return transaction.Commit()
}

func (s *store) writeConsumerGroupMetricSamples(ctx context.Context, samples []consumerGroupMetricSample) error {
	if len(samples) == 0 {
		return nil
	}
	transaction, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer transaction.Rollback()
	statement, err := transaction.PrepareContext(ctx, `
		INSERT INTO consumer_group_metric_samples(
			recorded_at, connection_id, stream_key, group_name, consumer_count,
			pending, lag, lag_known, last_delivered_id, consume_delay_ms,
			consumed_total, consume_rate, lag_delta
		) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
	`)
	if err != nil {
		return err
	}
	defer statement.Close()
	for _, sample := range samples {
		if _, err := statement.ExecContext(
			ctx,
			sample.RecordedAt.UTC().Format(time.RFC3339Nano),
			sample.ConnectionID,
			sample.StreamKey,
			sample.GroupName,
			sample.ConsumerCount,
			sample.Pending,
			sample.Lag,
			boolToInt(sample.LagKnown),
			sample.LastDeliveredID,
			nullableInt64(sample.ConsumeDelayMs),
			sample.ConsumedTotal,
			nullableFloat64(sample.ConsumeRate),
			nullableFloat64(sample.LagDelta),
		); err != nil {
			return err
		}
	}
	return transaction.Commit()
}

func nullableInt64(value *int64) any {
	if value == nil {
		return nil
	}
	return *value
}

func nullableFloat64(value *float64) any {
	if value == nil {
		return nil
	}
	return *value
}

func (s *store) latestMetricStates(ctx context.Context, connectionID string) (map[string]streamMetricState, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT samples.stream_key, samples.recorded_at, samples.last_delivered_id,
			samples.consume_delay_ms, samples.published_total, samples.consumed_total,
			samples.total_lag, samples.lag_known
		FROM stream_metric_samples AS samples
		JOIN (
			SELECT stream_key, MAX(id) AS id
			FROM stream_metric_samples
			WHERE connection_id=? AND stream_key<>''
			GROUP BY stream_key
		) AS latest ON latest.id=samples.id
	`, connectionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	states := make(map[string]streamMetricState)
	for rows.Next() {
		var streamKey, recordedAt, lastDeliveredID string
		var delay sql.NullInt64
		var state streamMetricState
		var lagKnown int
		if err := rows.Scan(
			&streamKey,
			&recordedAt,
			&lastDeliveredID,
			&delay,
			&state.PublishedTotal,
			&state.ConsumedTotal,
			&state.TotalLag,
			&lagKnown,
		); err != nil {
			return nil, err
		}
		state.RecordedAt, _ = time.Parse(time.RFC3339Nano, recordedAt)
		state.LastDeliveredID = lastDeliveredID
		state.LagKnown = lagKnown == 1
		if delay.Valid {
			value := delay.Int64
			state.ConsumeDelayMs = &value
		}
		states[streamKey] = state
	}
	return states, rows.Err()
}

func (s *store) latestConsumerGroupMetricStates(ctx context.Context, connectionID string) (map[string]consumerGroupMetricState, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT samples.stream_key, samples.group_name, samples.recorded_at,
			samples.last_delivered_id, samples.consume_delay_ms,
			samples.consumed_total, samples.lag, samples.lag_known
		FROM consumer_group_metric_samples AS samples
		JOIN (
			SELECT stream_key, group_name, MAX(id) AS id
			FROM consumer_group_metric_samples
			WHERE connection_id=?
			GROUP BY stream_key, group_name
		) AS latest ON latest.id=samples.id
	`, connectionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	states := make(map[string]consumerGroupMetricState)
	for rows.Next() {
		var streamKey, groupName, recordedAt string
		var delay sql.NullInt64
		var state consumerGroupMetricState
		var lagKnown int
		if err := rows.Scan(
			&streamKey,
			&groupName,
			&recordedAt,
			&state.LastDeliveredID,
			&delay,
			&state.ConsumedTotal,
			&state.Lag,
			&lagKnown,
		); err != nil {
			return nil, err
		}
		state.RecordedAt, _ = time.Parse(time.RFC3339Nano, recordedAt)
		state.LagKnown = lagKnown == 1
		if delay.Valid {
			value := delay.Int64
			state.ConsumeDelayMs = &value
		}
		states[consumerGroupMetricStateKey(streamKey, groupName)] = state
	}
	return states, rows.Err()
}

func consumerGroupMetricStateKey(streamKey, groupName string) string {
	return streamKey + "\x00" + groupName
}

func (s *store) maintainMetricSamples(ctx context.Context, now time.Time) error {
	minute := now.UTC().Truncate(time.Minute)
	from := minute.Add(-metricRawRetention)
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO stream_metric_rollups(
			recorded_at, connection_id, stream_key, entries, consumer_groups,
			consumer_count, total_lag, lag_known, pending, consume_delay_ms,
			redis_latency_ms, publish_rate, consume_rate, lag_delta
		)
		SELECT
			strftime('%Y-%m-%dT%H:%M:00Z', recorded_at),
			connection_id,
			stream_key,
			AVG(entries),
			AVG(consumer_groups),
			AVG(consumer_count),
			AVG(total_lag),
			MIN(lag_known),
			AVG(pending),
			AVG(consume_delay_ms),
			AVG(redis_latency_ms),
			AVG(publish_rate),
			AVG(consume_rate),
			AVG(lag_delta)
		FROM stream_metric_samples
		WHERE recorded_at>=? AND recorded_at<?
		GROUP BY strftime('%Y-%m-%dT%H:%M:00Z', recorded_at), connection_id, stream_key
		ON CONFLICT(connection_id, stream_key, recorded_at) DO UPDATE SET
			entries=excluded.entries,
			consumer_groups=excluded.consumer_groups,
			consumer_count=excluded.consumer_count,
			total_lag=excluded.total_lag,
			lag_known=excluded.lag_known,
			pending=excluded.pending,
			consume_delay_ms=excluded.consume_delay_ms,
			redis_latency_ms=excluded.redis_latency_ms,
			publish_rate=excluded.publish_rate,
			consume_rate=excluded.consume_rate,
			lag_delta=excluded.lag_delta
	`, from.Format(time.RFC3339Nano), minute.Format(time.RFC3339Nano))
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO consumer_group_metric_rollups(
			recorded_at, connection_id, stream_key, group_name, consumer_count,
			pending, lag, lag_known, consume_delay_ms, consume_rate, lag_delta
		)
		SELECT
			strftime('%Y-%m-%dT%H:%M:00Z', recorded_at),
			connection_id,
			stream_key,
			group_name,
			AVG(consumer_count),
			AVG(pending),
			AVG(lag),
			MIN(lag_known),
			AVG(consume_delay_ms),
			AVG(consume_rate),
			AVG(lag_delta)
		FROM consumer_group_metric_samples
		WHERE recorded_at>=? AND recorded_at<?
		GROUP BY strftime('%Y-%m-%dT%H:%M:00Z', recorded_at), connection_id, stream_key, group_name
		ON CONFLICT(connection_id, stream_key, group_name, recorded_at) DO UPDATE SET
			consumer_count=excluded.consumer_count,
			pending=excluded.pending,
			lag=excluded.lag,
			lag_known=excluded.lag_known,
			consume_delay_ms=excluded.consume_delay_ms,
			consume_rate=excluded.consume_rate,
			lag_delta=excluded.lag_delta
	`, from.Format(time.RFC3339Nano), minute.Format(time.RFC3339Nano))
	if err != nil {
		return err
	}
	if _, err := s.db.ExecContext(ctx, `DELETE FROM stream_metric_samples WHERE recorded_at<?`, now.Add(-metricRawRetention).UTC().Format(time.RFC3339Nano)); err != nil {
		return err
	}
	if _, err := s.db.ExecContext(ctx, `DELETE FROM consumer_group_metric_samples WHERE recorded_at<?`, now.Add(-metricRawRetention).UTC().Format(time.RFC3339Nano)); err != nil {
		return err
	}
	if _, err := s.db.ExecContext(ctx, `DELETE FROM stream_metric_rollups WHERE recorded_at<?`, now.Add(-metricRetention).UTC().Format(time.RFC3339Nano)); err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `DELETE FROM consumer_group_metric_rollups WHERE recorded_at<?`, now.Add(-metricRetention).UTC().Format(time.RFC3339Nano))
	return err
}

func (s *store) listMetricSeries(
	ctx context.Context,
	connectionID string,
	streamKey string,
	from time.Time,
	until time.Time,
	maxPoints int,
) ([]streamMetricPoint, error) {
	if maxPoints < 1 {
		maxPoints = metricMaxPoints
	}
	rangeSeconds := math.Max(1, until.Sub(from).Seconds())
	bucketSeconds := int64(math.Ceil(rangeSeconds / float64(maxPoints)))
	if bucketSeconds < 1 {
		bucketSeconds = 1
	}
	table := "stream_metric_samples"
	if until.Sub(from) > time.Hour {
		table = "stream_metric_rollups"
		if bucketSeconds < 60 {
			bucketSeconds = 60
		}
	}
	where := `connection_id=? AND recorded_at>=? AND recorded_at<=?`
	args := []any{
		connectionID,
		from.UTC().Format(time.RFC3339Nano),
		until.UTC().Format(time.RFC3339Nano),
	}
	if streamKey != "" {
		where += ` AND stream_key=?`
		args = append(args, streamKey)
	}
	args = append(args, bucketSeconds)
	rows, err := s.db.QueryContext(ctx, `
		WITH snapshots AS (
			SELECT
				unixepoch(recorded_at) AS sample_time,
				SUM(CASE WHEN stream_key<>'' THEN entries ELSE 0 END) AS entries,
				SUM(CASE WHEN stream_key<>'' THEN consumer_groups ELSE 0 END) AS consumer_groups,
				SUM(CASE WHEN stream_key<>'' THEN consumer_count ELSE 0 END) AS consumer_count,
				CASE
					WHEN SUM(CASE WHEN stream_key<>'' AND lag_known=0 THEN 1 ELSE 0 END)>0 THEN NULL
					ELSE SUM(CASE WHEN stream_key<>'' THEN total_lag ELSE 0 END)
				END AS total_lag,
				SUM(CASE WHEN stream_key<>'' THEN pending ELSE 0 END) AS pending,
				AVG(CASE WHEN stream_key<>'' THEN consume_delay_ms END) AS consume_delay_ms,
				MAX(redis_latency_ms) AS redis_latency_ms,
				SUM(CASE WHEN stream_key<>'' THEN publish_rate END) AS publish_rate,
				SUM(CASE WHEN stream_key<>'' THEN consume_rate END) AS consume_rate,
				SUM(CASE WHEN stream_key<>'' THEN lag_delta END) AS lag_delta
			FROM `+table+`
			WHERE `+where+`
			GROUP BY recorded_at
		),
		buckets AS (
			SELECT
				CAST(sample_time / ? AS INTEGER) AS bucket,
				MIN(sample_time) AS sample_time,
				AVG(entries) AS entries,
				AVG(consumer_groups) AS consumer_groups,
				AVG(consumer_count) AS consumer_count,
				AVG(total_lag) AS total_lag,
				AVG(pending) AS pending,
				AVG(consume_delay_ms) AS consume_delay_ms,
				AVG(redis_latency_ms) AS redis_latency_ms,
				AVG(publish_rate) AS publish_rate,
				AVG(consume_rate) AS consume_rate,
				AVG(lag_delta) AS lag_delta
			FROM snapshots
			GROUP BY bucket
		)
		SELECT sample_time, entries, consumer_groups, consumer_count, total_lag,
			pending, consume_delay_ms, redis_latency_ms, publish_rate, consume_rate,
			lag_delta
		FROM buckets
		ORDER BY sample_time
	`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]streamMetricPoint, 0)
	for rows.Next() {
		var timestamp int64
		var point streamMetricPoint
		var totalLag, consumeDelay, publishRate, consumeRate, lagDelta sql.NullFloat64
		if err := rows.Scan(
			&timestamp,
			&point.Entries,
			&point.ConsumerGroups,
			&point.ConsumerCount,
			&totalLag,
			&point.Pending,
			&consumeDelay,
			&point.RedisLatencyMs,
			&publishRate,
			&consumeRate,
			&lagDelta,
		); err != nil {
			return nil, err
		}
		point.Timestamp = time.Unix(timestamp, 0).UTC()
		point.TotalLag = nullFloatPointer(totalLag)
		point.ConsumeDelayMs = nullFloatPointer(consumeDelay)
		point.PublishRate = nullFloatPointer(publishRate)
		point.ConsumeRate = nullFloatPointer(consumeRate)
		point.LagDelta = nullFloatPointer(lagDelta)
		items = append(items, point)
	}
	return items, rows.Err()
}

func (s *store) listConsumerGroupMetricSeries(
	ctx context.Context,
	connectionID string,
	streamKey string,
	from time.Time,
	until time.Time,
	maxPoints int,
) ([]string, []consumerGroupMetricPoint, error) {
	if maxPoints < 1 {
		maxPoints = metricMaxPoints
	}
	rangeSeconds := math.Max(1, until.Sub(from).Seconds())
	bucketSeconds := int64(math.Ceil(rangeSeconds / float64(maxPoints)))
	if bucketSeconds < 1 {
		bucketSeconds = 1
	}
	table := "consumer_group_metric_samples"
	if until.Sub(from) > time.Hour {
		table = "consumer_group_metric_rollups"
		if bucketSeconds < 60 {
			bucketSeconds = 60
		}
	}
	rows, err := s.db.QueryContext(ctx, `
		WITH buckets AS (
			SELECT
				group_name,
				CAST(unixepoch(recorded_at) / ? AS INTEGER) AS bucket,
				MIN(unixepoch(recorded_at)) AS sample_time,
				AVG(consumer_count) AS consumer_count,
				AVG(pending) AS pending,
				CASE WHEN MIN(lag_known)=0 THEN NULL ELSE AVG(lag) END AS lag,
				AVG(consume_delay_ms) AS consume_delay_ms,
				AVG(consume_rate) AS consume_rate,
				AVG(lag_delta) AS lag_delta
			FROM `+table+`
			WHERE connection_id=? AND stream_key=? AND recorded_at>=? AND recorded_at<=?
			GROUP BY group_name, bucket
		)
		SELECT sample_time, group_name, consumer_count, pending, lag,
			consume_delay_ms, consume_rate, lag_delta
		FROM buckets
		ORDER BY sample_time, group_name
	`, bucketSeconds, connectionID, streamKey, from.UTC().Format(time.RFC3339Nano), until.UTC().Format(time.RFC3339Nano))
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	groupSet := make(map[string]struct{})
	pointIndexes := make(map[int64]int)
	points := make([]consumerGroupMetricPoint, 0)
	for rows.Next() {
		var timestamp int64
		var groupName string
		var value consumerGroupMetricValue
		var lag, consumeDelay, consumeRate, lagDelta sql.NullFloat64
		if err := rows.Scan(
			&timestamp,
			&groupName,
			&value.ConsumerCount,
			&value.Pending,
			&lag,
			&consumeDelay,
			&consumeRate,
			&lagDelta,
		); err != nil {
			return nil, nil, err
		}
		value.Lag = nullFloatPointer(lag)
		value.ConsumeDelayMs = nullFloatPointer(consumeDelay)
		value.ConsumeRate = nullFloatPointer(consumeRate)
		value.LagDelta = nullFloatPointer(lagDelta)
		index, exists := pointIndexes[timestamp]
		if !exists {
			index = len(points)
			pointIndexes[timestamp] = index
			points = append(points, consumerGroupMetricPoint{
				Timestamp: time.Unix(timestamp, 0).UTC(),
				Values:    make(map[string]consumerGroupMetricValue),
			})
		}
		points[index].Values[groupName] = value
		groupSet[groupName] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	groups := make([]string, 0, len(groupSet))
	for groupName := range groupSet {
		groups = append(groups, groupName)
	}
	sort.Strings(groups)
	return groups, points, nil
}

func nullFloatPointer(value sql.NullFloat64) *float64 {
	if !value.Valid {
		return nil
	}
	result := value.Float64
	return &result
}

func boolToInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

func (s *apiServer) startMetricCollection(ctx context.Context) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		maintenanceContext, cancel := context.WithTimeout(ctx, 10*time.Second)
		if err := s.store.maintainMetricSamples(maintenanceContext, time.Now()); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf(`{"level":"warn","message":"Unable to maintain stream metrics","error":%q}`, err)
		}
		cancel()
		s.collectMetricSnapshots(ctx)
		collectionTicker := time.NewTicker(metricCollectionInterval)
		defer collectionTicker.Stop()
		maintenanceTicker := time.NewTicker(time.Minute)
		defer maintenanceTicker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-collectionTicker.C:
				s.collectMetricSnapshots(ctx)
			case <-maintenanceTicker.C:
				maintenanceContext, cancel := context.WithTimeout(ctx, 10*time.Second)
				if err := s.store.maintainMetricSamples(maintenanceContext, time.Now()); err != nil && !errors.Is(err, context.Canceled) {
					log.Printf(`{"level":"warn","message":"Unable to maintain stream metrics","error":%q}`, err)
				}
				cancel()
			}
		}
	}()
	return done
}

func (s *apiServer) collectMetricSnapshots(parent context.Context) {
	for _, connectionID := range s.redis.ids() {
		if parent.Err() != nil {
			return
		}
		connection, err := s.redis.get(connectionID)
		if err != nil {
			continue
		}
		ctx, cancel := context.WithTimeout(parent, 900*time.Millisecond)
		startedAt := time.Now()
		if err := connection.client.Ping(ctx).Err(); err != nil {
			cancel()
			continue
		}
		latencyMs := float64(time.Since(startedAt).Microseconds()) / 1000
		monitored, err := s.store.listMonitoredStreams(ctx, connectionID)
		if err != nil {
			cancel()
			continue
		}
		states, err := s.store.latestMetricStates(ctx, connectionID)
		if err != nil {
			cancel()
			continue
		}
		groupStates, err := s.store.latestConsumerGroupMetricStates(ctx, connectionID)
		if err != nil {
			cancel()
			continue
		}
		recordedAt := time.Now().UTC()
		samples, groupSamples := collectConnectionMetricSamples(ctx, connection, monitored, states, groupStates, recordedAt, latencyMs)
		if len(samples) == 0 {
			samples = append(samples, streamMetricSample{
				RecordedAt: recordedAt, ConnectionID: connectionID, StreamKey: "",
				LagKnown: true, RedisLatencyMs: latencyMs,
			})
		}
		if err := s.store.writeMetricSamples(ctx, samples); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf(`{"level":"warn","message":"Unable to store stream metrics","connection":%q,"error":%q}`, connectionID, err)
		}
		if err := s.store.writeConsumerGroupMetricSamples(ctx, groupSamples); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf(`{"level":"warn","message":"Unable to store consumer group metrics","connection":%q,"error":%q}`, connectionID, err)
		}
		cancel()
	}
}

func collectConnectionMetricSamples(
	ctx context.Context,
	connection *managedRedis,
	monitored []monitoredStreamRecord,
	states map[string]streamMetricState,
	groupStates map[string]consumerGroupMetricState,
	recordedAt time.Time,
	latencyMs float64,
) ([]streamMetricSample, []consumerGroupMetricSample) {
	streamCommands := make([]*redis.XInfoStreamCmd, len(monitored))
	groupCommands := make([]*redis.XInfoGroupsCmd, len(monitored))
	_, _ = connection.client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for index, item := range monitored {
			streamCommands[index] = pipe.XInfoStream(ctx, item.Key)
			groupCommands[index] = pipe.XInfoGroups(ctx, item.Key)
		}
		return nil
	})
	samples := make([]streamMetricSample, 0, len(monitored))
	groupSamples := make([]consumerGroupMetricSample, 0)
	for index, monitoredStream := range monitored {
		streamInfo, err := streamCommands[index].Result()
		if err != nil {
			continue
		}
		groups, err := groupCommands[index].Result()
		if err != nil && !errors.Is(err, redis.Nil) {
			continue
		}
		groupCount, totalLag, lagKnown, pending, lastConsumed := summarizeOverviewGroups(groups)
		publishedTotal := streamInfo.EntriesAdded
		if publishedTotal < streamInfo.Length {
			publishedTotal = streamInfo.Length
		}
		consumedTotal, consumerCount, consumedKnown := summarizeConsumerProgress(groups)
		previous := states[monitoredStream.Key]
		publishRate, consumeRate, lagDelta := metricRates(recordedAt, previous, publishedTotal, consumedTotal, totalLag, consumedKnown, lagKnown)
		samples = append(samples, streamMetricSample{
			RecordedAt:      recordedAt,
			ConnectionID:    connection.config.ID,
			StreamKey:       monitoredStream.Key,
			Entries:         streamInfo.Length,
			ConsumerGroups:  groupCount,
			ConsumerCount:   consumerCount,
			TotalLag:        totalLag,
			LagKnown:        lagKnown,
			Pending:         pending,
			LastDeliveredID: lastConsumed,
			ConsumeDelayMs:  observedConsumeDelay(recordedAt, lastConsumed, previous),
			RedisLatencyMs:  latencyMs,
			PublishedTotal:  publishedTotal,
			ConsumedTotal:   consumedTotal,
			PublishRate:     publishRate,
			ConsumeRate:     consumeRate,
			LagDelta:        lagDelta,
		})
		for _, group := range groups {
			lag := group.Lag
			lagKnown := lag >= 0
			if !lagKnown {
				lag = 0
			}
			consumedTotal := group.EntriesRead
			consumedKnown := consumedTotal >= 0
			if !consumedKnown {
				consumedTotal = 0
			}
			previousGroup := groupStates[consumerGroupMetricStateKey(monitoredStream.Key, group.Name)]
			consumeRate, groupLagDelta := consumerGroupMetricRates(
				recordedAt,
				previousGroup,
				consumedTotal,
				lag,
				consumedKnown,
				lagKnown,
			)
			groupSamples = append(groupSamples, consumerGroupMetricSample{
				RecordedAt:      recordedAt,
				ConnectionID:    connection.config.ID,
				StreamKey:       monitoredStream.Key,
				GroupName:       group.Name,
				ConsumerCount:   group.Consumers,
				Pending:         group.Pending,
				Lag:             lag,
				LagKnown:        lagKnown,
				LastDeliveredID: group.LastDeliveredID,
				ConsumeDelayMs: observedConsumeDelay(recordedAt, group.LastDeliveredID, streamMetricState{
					LastDeliveredID: previousGroup.LastDeliveredID,
					ConsumeDelayMs:  previousGroup.ConsumeDelayMs,
				}),
				ConsumedTotal: consumedTotal,
				ConsumeRate:   consumeRate,
				LagDelta:      groupLagDelta,
			})
		}
	}
	return samples, groupSamples
}

func consumerGroupMetricRates(
	recordedAt time.Time,
	previous consumerGroupMetricState,
	consumedTotal int64,
	lag int64,
	consumeKnown bool,
	lagKnown bool,
) (*float64, *float64) {
	elapsed := recordedAt.Sub(previous.RecordedAt).Seconds()
	if previous.RecordedAt.IsZero() || elapsed <= 0 || elapsed > 10 {
		return nil, nil
	}
	var consumeRate *float64
	if consumeKnown {
		value := math.Max(0, float64(consumedTotal-previous.ConsumedTotal)/elapsed)
		consumeRate = &value
	}
	var lagDelta *float64
	if previous.LagKnown && lagKnown {
		value := float64(lag-previous.Lag) / elapsed
		lagDelta = &value
	}
	return consumeRate, lagDelta
}

func summarizeConsumerProgress(groups []redis.XInfoGroup) (int64, int64, bool) {
	var consumedTotal, consumerCount int64
	known := true
	for _, group := range groups {
		consumerCount += group.Consumers
		if group.EntriesRead < 0 {
			known = false
			continue
		}
		consumedTotal += group.EntriesRead
	}
	return consumedTotal, consumerCount, known
}

func metricRates(
	recordedAt time.Time,
	previous streamMetricState,
	publishedTotal int64,
	consumedTotal int64,
	totalLag int64,
	consumeKnown bool,
	lagKnown bool,
) (*float64, *float64, *float64) {
	elapsed := recordedAt.Sub(previous.RecordedAt).Seconds()
	if previous.RecordedAt.IsZero() || elapsed <= 0 || elapsed > 10 {
		return nil, nil, nil
	}
	published := math.Max(0, float64(publishedTotal-previous.PublishedTotal)/elapsed)
	var consumed *float64
	if consumeKnown {
		value := math.Max(0, float64(consumedTotal-previous.ConsumedTotal)/elapsed)
		consumed = &value
	}
	var lagDelta *float64
	if previous.LagKnown && lagKnown {
		value := float64(totalLag-previous.TotalLag) / elapsed
		lagDelta = &value
	}
	return &published, consumed, lagDelta
}

func observedConsumeDelay(recordedAt time.Time, lastDeliveredID string, previous streamMetricState) *int64 {
	if lastDeliveredID == "" || lastDeliveredID == "0-0" {
		return nil
	}
	if previous.LastDeliveredID == "" || previous.LastDeliveredID == lastDeliveredID {
		return previous.ConsumeDelayMs
	}
	millis, err := strconv.ParseInt(strings.SplitN(lastDeliveredID, "-", 2)[0], 10, 64)
	if err != nil || millis <= 0 {
		return previous.ConsumeDelayMs
	}
	publishedAt := time.UnixMilli(millis)
	if publishedAt.After(recordedAt.Add(5 * time.Minute)) {
		return previous.ConsumeDelayMs
	}
	delay := recordedAt.Sub(publishedAt).Milliseconds()
	if delay < 0 {
		delay = 0
	}
	return &delay
}

func metricRange(value string) (string, time.Duration, bool) {
	switch value {
	case "1m":
		return value, time.Minute, true
	case "5m", "":
		return "5m", 5 * time.Minute, true
	case "15m":
		return value, 15 * time.Minute, true
	case "1h":
		return value, time.Hour, true
	case "6h":
		return value, 6 * time.Hour, true
	case "24h":
		return value, 24 * time.Hour, true
	case "7d":
		return value, metricRetention, true
	default:
		return "", 0, false
	}
}

func (s *apiServer) metricSeries(writer http.ResponseWriter, request *http.Request) {
	connection, err := s.redis.get(request.URL.Query().Get("connectionId"))
	if err != nil {
		writeError(writer, http.StatusBadRequest, "unknown_connection", err.Error())
		return
	}
	rangeName, duration, ok := metricRange(request.URL.Query().Get("range"))
	if !ok {
		writeError(writer, http.StatusBadRequest, "invalid_range", "range must be one of 1m, 5m, 15m, 1h, 6h, 24h or 7d")
		return
	}
	streamKey := strings.TrimSpace(request.URL.Query().Get("streamKey"))
	until := time.Now().UTC()
	items, err := s.store.listMetricSeries(request.Context(), connection.config.ID, streamKey, until.Add(-duration), until, metricMaxPoints)
	if err != nil {
		writeError(writer, http.StatusInternalServerError, "metrics_failed", "unable to load stream metrics")
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{
		"connectionId":    connection.config.ID,
		"streamKey":       streamKey,
		"range":           rangeName,
		"intervalSeconds": int(metricCollectionInterval.Seconds()),
		"generatedAt":     until,
		"items":           items,
	})
}

func (s *apiServer) consumerGroupMetricSeries(writer http.ResponseWriter, request *http.Request) {
	connection, err := s.redis.get(request.URL.Query().Get("connectionId"))
	if err != nil {
		writeError(writer, http.StatusBadRequest, "unknown_connection", err.Error())
		return
	}
	streamKey, err := validateMonitoredStreamKey(request.URL.Query().Get("streamKey"))
	if err != nil {
		writeError(writer, http.StatusBadRequest, "invalid_stream_key", err.Error())
		return
	}
	rangeName, duration, ok := metricRange(request.URL.Query().Get("range"))
	if !ok {
		writeError(writer, http.StatusBadRequest, "invalid_range", "range must be one of 1m, 5m, 15m, 1h, 6h, 24h or 7d")
		return
	}
	until := time.Now().UTC()
	groups, items, err := s.store.listConsumerGroupMetricSeries(
		request.Context(),
		connection.config.ID,
		streamKey,
		until.Add(-duration),
		until,
		metricMaxPoints,
	)
	if err != nil {
		writeError(writer, http.StatusInternalServerError, "metrics_failed", "unable to load consumer group metrics")
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{
		"connectionId":    connection.config.ID,
		"streamKey":       streamKey,
		"range":           rangeName,
		"intervalSeconds": int(metricCollectionInterval.Seconds()),
		"generatedAt":     until,
		"groups":          groups,
		"items":           items,
	})
}
