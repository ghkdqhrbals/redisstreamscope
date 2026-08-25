package main

import (
	"context"
	"time"
)

const (
	lifecycleHistoryRetention = 7 * 24 * time.Hour
	alertHistoryRetention     = 30 * 24 * time.Hour
)

// maintainOperationalHistory bounds the SQLite tables that grow with runtime
// traffic. SQLite can reuse the released pages, so this prevents unbounded
// volume growth without running a disruptive VACUUM in the monitoring loop.
func (s *store) maintainOperationalHistory(ctx context.Context, now time.Time) error {
	now = now.UTC()
	transaction, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer transaction.Rollback()

	if _, err := transaction.ExecContext(ctx,
		`DELETE FROM request_lifecycles WHERE updated_at<?`,
		now.Add(-lifecycleHistoryRetention).UnixNano(),
	); err != nil {
		return err
	}
	if _, err := transaction.ExecContext(ctx,
		`DELETE FROM trace_spans WHERE updated_at<?`,
		now.Add(-lifecycleHistoryRetention).UnixNano(),
	); err != nil {
		return err
	}
	if _, err := transaction.ExecContext(ctx,
		`DELETE FROM stream_schema_snapshots WHERE captured_at<?`,
		now.Add(-90*24*time.Hour).UnixNano(),
	); err != nil {
		return err
	}
	alertCutoff := formatAlertTime(now.Add(-alertHistoryRetention))
	if _, err := transaction.ExecContext(ctx,
		`DELETE FROM alert_webhook_deliveries WHERE completed_at<?`, alertCutoff,
	); err != nil {
		return err
	}
	if _, err := transaction.ExecContext(ctx,
		`DELETE FROM alert_incidents WHERE resolved_at IS NOT NULL AND resolved_at<?`, alertCutoff,
	); err != nil {
		return err
	}
	if _, err := transaction.ExecContext(ctx,
		`DELETE FROM alert_rules WHERE deleted_at IS NOT NULL AND deleted_at<?`, alertCutoff,
	); err != nil {
		return err
	}
	if err := transaction.Commit(); err != nil {
		return err
	}
	return s.pruneRecoveryHistory(ctx, now)
}
