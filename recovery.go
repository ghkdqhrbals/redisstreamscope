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
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	recoveryMaxCount             = 500
	recoveryRequestBodyLimit     = 256 << 10
	recoveryMaxEntryPayloadBytes = 128 << 10
	recoveryMaxBatchPayloadBytes = 4 << 20
	recoveryMaxFieldCount        = 1024
	quarantineListMax            = 50
	recoveryMarkerTTLSeconds     = int64((30 * 24 * time.Hour) / time.Second)
	// Redis Streams and XINFO GROUPS were introduced in Redis 5.0. Keeping the
	// script to Redis Lua 5.1 syntax makes the atomic compare-and-set portable
	// across every Redis version supported by RedisStreamScope.
	atomicGroupSetIDScript = `
local groups = redis.call('XINFO', 'GROUPS', KEYS[1])
for _, info in ipairs(groups) do
  local name = nil
  local current = nil
  for index = 1, #info, 2 do
    if info[index] == 'name' then
      name = info[index + 1]
    elseif info[index] == 'last-delivered-id' then
      current = info[index + 1]
    end
  end
  if name == ARGV[1] then
    if current ~= ARGV[2] then
      return {0, current}
    end
    redis.call('XGROUP', 'SETID', KEYS[1], ARGV[1], ARGV[3])
    return {1, ARGV[3]}
  end
end
return {-1, ''}
`
)

var (
	errIdempotencyConflict  = errors.New("idempotency key was already used for a different request")
	errExecutionRunning     = errors.New("an execution with this idempotency key is still running or has an unknown outcome")
	errGroupRevisionChanged = errors.New("consumer group offset changed after the recovery plan was prepared")
	redisSlotTagCache       sync.Map
)

type recoveryPlanRequest struct {
	Action       string   `json:"action"`
	ConnectionID string   `json:"connectionId"`
	StreamKey    string   `json:"streamKey"`
	Group        string   `json:"group"`
	Consumer     string   `json:"consumer,omitempty"`
	IDs          []string `json:"ids,omitempty"`
	MinIdleMs    int64    `json:"minIdleMs,omitempty"`
	Start        string   `json:"start,omitempty"`
	Count        int64    `json:"count,omitempty"`
	TargetID     string   `json:"targetId,omitempty"`
	// ExpectedCurrentGroupID is populated from the read-only XGROUP SETID
	// preview and covered by both the confirmation and idempotency hashes.
	// Execution rejects the plan if Redis no longer reports this offset.
	ExpectedCurrentGroupID string `json:"expectedCurrentGroupId,omitempty"`
}

type recoveryExecutionRequest struct {
	recoveryPlanRequest
	Confirmation   string `json:"confirmation"`
	IdempotencyKey string `json:"idempotencyKey"`
}

type recoveryPendingEntry struct {
	ID            string `json:"id"`
	Consumer      string `json:"consumer"`
	IdleMs        int64  `json:"idleMs"`
	DeliveryCount int64  `json:"deliveryCount"`
	Eligible      bool   `json:"eligible"`
}

type recoveryPlanResponse struct {
	Action                 string                 `json:"action"`
	ConnectionID           string                 `json:"connectionId"`
	StreamKey              string                 `json:"streamKey"`
	Group                  string                 `json:"group"`
	RequiredPermission     string                 `json:"requiredPermission"`
	Scope                  string                 `json:"scope"`
	RequestedCount         int                    `json:"requestedCount"`
	CandidateCount         int                    `json:"candidateCount"`
	Pending                []recoveryPendingEntry `json:"pending"`
	CurrentGroupID         string                 `json:"currentGroupId,omitempty"`
	ExpectedCurrentGroupID string                 `json:"expectedCurrentGroupId,omitempty"`
	TargetGroupID          string                 `json:"targetGroupId,omitempty"`
	Confirmation           string                 `json:"confirmation"`
	Warnings               []string               `json:"warnings"`
	Guarantees             []string               `json:"guarantees"`
}

type recoveryExecutionResponse struct {
	OK               bool     `json:"ok"`
	Action           string   `json:"action"`
	Affected         int64    `json:"affected"`
	MessageIDs       []string `json:"messageIds,omitempty"`
	NextStart        string   `json:"nextStart,omitempty"`
	TargetGroupID    string   `json:"targetGroupId,omitempty"`
	IdempotentReplay bool     `json:"idempotentReplay,omitempty"`
}

type recoveryPlanRedis interface {
	XPendingExt(context.Context, *redis.XPendingExtArgs) *redis.XPendingExtCmd
	XInfoGroups(context.Context, string) *redis.XInfoGroupsCmd
}

type recoveryExecutionRedis interface {
	XAck(context.Context, string, string, ...string) *redis.IntCmd
	XClaim(context.Context, *redis.XClaimArgs) *redis.XMessageSliceCmd
	XAutoClaim(context.Context, *redis.XAutoClaimArgs) *redis.XAutoClaimCmd
	Eval(context.Context, string, []string, ...interface{}) *redis.Cmd
}

// normalizeRecoveryRequest is deliberately strict: recovery commands have a
// bounded blast radius and the exact normalized request is covered by the
// confirmation token and idempotency hash.
func normalizeRecoveryRequest(input recoveryPlanRequest) (recoveryPlanRequest, error) {
	input.Action = strings.ToLower(strings.TrimSpace(input.Action))
	input.ConnectionID = strings.TrimSpace(input.ConnectionID)
	input.StreamKey = strings.TrimSpace(input.StreamKey)
	input.Group = strings.TrimSpace(input.Group)
	input.Consumer = strings.TrimSpace(input.Consumer)
	input.Start = strings.TrimSpace(input.Start)
	input.TargetID = strings.TrimSpace(input.TargetID)
	input.ExpectedCurrentGroupID = strings.TrimSpace(input.ExpectedCurrentGroupID)
	if err := validateRecoveryIdentifier("connectionId", input.ConnectionID, 256); err != nil {
		return input, err
	}
	if err := validateRecoveryIdentifier("streamKey", input.StreamKey, 1024); err != nil {
		return input, err
	}
	if err := validateRecoveryIdentifier("group", input.Group, 256); err != nil {
		return input, err
	}
	if input.MinIdleMs < 0 || input.MinIdleMs > int64((365*24*time.Hour)/time.Millisecond) {
		return input, errors.New("minIdleMs must be between 0 and 31536000000")
	}

	deduplicated := make([]string, 0, len(input.IDs))
	seen := make(map[string]struct{}, len(input.IDs))
	for _, rawID := range input.IDs {
		id := strings.TrimSpace(rawID)
		if err := validateStreamEntryID("id", id, false); err != nil {
			return input, err
		}
		if _, exists := seen[id]; exists {
			continue
		}
		seen[id] = struct{}{}
		deduplicated = append(deduplicated, id)
	}
	if len(deduplicated) > recoveryMaxCount {
		return input, fmt.Errorf("ids cannot contain more than %d entries", recoveryMaxCount)
	}
	input.IDs = deduplicated

	switch input.Action {
	case "xack":
		if len(input.IDs) == 0 {
			return input, errors.New("ids are required for xack")
		}
	case "xclaim":
		if len(input.IDs) == 0 {
			return input, errors.New("ids are required for xclaim")
		}
		if err := validateRecoveryIdentifier("consumer", input.Consumer, 256); err != nil {
			return input, err
		}
	case "xautoclaim":
		if err := validateRecoveryIdentifier("consumer", input.Consumer, 256); err != nil {
			return input, err
		}
		if input.Start == "" {
			input.Start = "0-0"
		}
		if err := validateStreamEntryID("start", input.Start, false); err != nil {
			return input, err
		}
		if input.Count == 0 {
			input.Count = 100
		}
		if input.Count < 1 || input.Count > recoveryMaxCount {
			return input, fmt.Errorf("count must be between 1 and %d", recoveryMaxCount)
		}
	case "xgroup-setid":
		if err := validateStreamEntryID("targetId", input.TargetID, true); err != nil {
			return input, err
		}
		if input.ExpectedCurrentGroupID != "" {
			if err := validateStreamEntryID("expectedCurrentGroupId", input.ExpectedCurrentGroupID, false); err != nil {
				return input, err
			}
		}
	default:
		return input, errors.New("action must be xack, xclaim, xautoclaim or xgroup-setid")
	}
	return input, nil
}

func validateRecoveryIdentifier(name, value string, maxBytes int) error {
	if value == "" {
		return fmt.Errorf("%s is required", name)
	}
	if len([]byte(value)) > maxBytes {
		return fmt.Errorf("%s must be %d bytes or fewer", name, maxBytes)
	}
	if strings.ContainsRune(value, '\x00') {
		return fmt.Errorf("%s cannot contain a null character", name)
	}
	return nil
}

func validateStreamEntryID(name, value string, allowSpecial bool) error {
	if allowSpecial && (value == "$" || value == "0") {
		return nil
	}
	if err := validateRecoveryIdentifier(name, value, 128); err != nil {
		return err
	}
	parts := strings.Split(value, "-")
	if len(parts) != 2 {
		return fmt.Errorf("%s must be a Redis stream ID", name)
	}
	if _, err := strconv.ParseUint(parts[0], 10, 64); err != nil {
		return fmt.Errorf("%s must be a Redis stream ID", name)
	}
	if _, err := strconv.ParseUint(parts[1], 10, 64); err != nil {
		return fmt.Errorf("%s must be a Redis stream ID", name)
	}
	return nil
}

func recoveryScope(input recoveryPlanRequest) string {
	return "stream:" + input.ConnectionID + ":" + input.StreamKey
}

func recoveryConfirmation(input recoveryPlanRequest) string {
	canonical, _ := json.Marshal(input)
	digest := sha256.Sum256(canonical)
	return "confirm:" + input.Action + ":" + hex.EncodeToString(digest[:8])
}

func recoveryRequestHash(kind string, value any) string {
	canonical, _ := json.Marshal(struct {
		Kind  string `json:"kind"`
		Value any    `json:"value"`
	}{Kind: kind, Value: value})
	digest := sha256.Sum256(canonical)
	return hex.EncodeToString(digest[:])
}

func buildRecoveryPlan(ctx context.Context, client recoveryPlanRedis, input recoveryPlanRequest) (recoveryPlanResponse, error) {
	result := recoveryPlanResponse{
		Action: input.Action, ConnectionID: input.ConnectionID, StreamKey: input.StreamKey, Group: input.Group,
		RequiredPermission: "groups:manage", Scope: recoveryScope(input),
		Pending: []recoveryPendingEntry{}, Warnings: []string{}, Guarantees: []string{
			"Planning only reads Redis and never changes the PEL, consumer ownership, acknowledgements, or group offset.",
			"Execution is bounded to at most 500 entries and is audited against the exact stream scope.",
		},
	}
	switch input.Action {
	case "xack", "xclaim":
		result.RequestedCount = len(input.IDs)
		pending, err := recoveryPendingByID(ctx, client, input.StreamKey, input.Group, input.IDs)
		if err != nil {
			return result, err
		}
		for _, item := range pending {
			item.Eligible = input.Action == "xack" || item.IdleMs >= input.MinIdleMs
			if item.Eligible {
				result.CandidateCount++
			}
			result.Pending = append(result.Pending, item)
		}
	case "xautoclaim":
		result.RequestedCount = int(input.Count)
		scanCount := input.Count * 10
		if scanCount > recoveryMaxCount {
			scanCount = recoveryMaxCount
		}
		items, err := client.XPendingExt(ctx, &redis.XPendingExtArgs{
			Stream: input.StreamKey, Group: input.Group, Start: input.Start, End: "+", Count: scanCount,
		}).Result()
		if err != nil {
			return result, err
		}
		for _, item := range items {
			if int64(len(result.Pending)) >= input.Count {
				break
			}
			eligible := item.Idle.Milliseconds() >= input.MinIdleMs
			if !eligible {
				continue
			}
			result.Pending = append(result.Pending, recoveryPendingEntry{
				ID: item.ID, Consumer: item.Consumer, IdleMs: item.Idle.Milliseconds(), DeliveryCount: item.RetryCount, Eligible: true,
			})
			result.CandidateCount++
		}
		result.Warnings = append(result.Warnings, "XAUTOCLAIM scans Redis at execution time; the eligible set and returned cursor can change after this preview.")
	case "xgroup-setid":
		groups, err := client.XInfoGroups(ctx, input.StreamKey).Result()
		if err != nil {
			return result, err
		}
		found := false
		for _, group := range groups {
			if group.Name == input.Group {
				result.CurrentGroupID = group.LastDeliveredID
				found = true
				break
			}
		}
		if !found {
			return result, errors.New("consumer group was not found")
		}
		input.ExpectedCurrentGroupID = result.CurrentGroupID
		result.ExpectedCurrentGroupID = result.CurrentGroupID
		result.RequestedCount = 1
		result.CandidateCount = 1
		result.TargetGroupID = input.TargetID
		result.Warnings = append(result.Warnings, "Changing the group ID can cause messages to be delivered again or skipped; it does not clear existing pending entries.")
	}
	result.Confirmation = recoveryConfirmation(input)
	return result, nil
}

func recoveryPendingByID(ctx context.Context, client recoveryPlanRedis, stream, group string, ids []string) ([]recoveryPendingEntry, error) {
	result := make([]recoveryPendingEntry, 0, len(ids))
	for _, id := range ids {
		items, err := client.XPendingExt(ctx, &redis.XPendingExtArgs{
			Stream: stream, Group: group, Start: id, End: id, Count: 1,
		}).Result()
		if err != nil {
			return nil, err
		}
		if len(items) == 0 || items[0].ID != id {
			continue
		}
		item := items[0]
		result = append(result, recoveryPendingEntry{
			ID: item.ID, Consumer: item.Consumer, IdleMs: item.Idle.Milliseconds(), DeliveryCount: item.RetryCount,
		})
	}
	return result, nil
}

func executeRecoveryCommand(ctx context.Context, client recoveryExecutionRedis, input recoveryPlanRequest) (recoveryExecutionResponse, error) {
	result := recoveryExecutionResponse{OK: true, Action: input.Action}
	switch input.Action {
	case "xack":
		affected, err := client.XAck(ctx, input.StreamKey, input.Group, input.IDs...).Result()
		result.Affected = affected
		return result, err
	case "xclaim":
		messages, err := client.XClaim(ctx, &redis.XClaimArgs{
			Stream: input.StreamKey, Group: input.Group, Consumer: input.Consumer,
			MinIdle: time.Duration(input.MinIdleMs) * time.Millisecond, Messages: input.IDs,
		}).Result()
		for _, message := range messages {
			result.MessageIDs = append(result.MessageIDs, message.ID)
		}
		result.Affected = int64(len(result.MessageIDs))
		return result, err
	case "xautoclaim":
		messages, next, err := client.XAutoClaim(ctx, &redis.XAutoClaimArgs{
			Stream: input.StreamKey, Group: input.Group, Consumer: input.Consumer,
			MinIdle: time.Duration(input.MinIdleMs) * time.Millisecond, Start: input.Start, Count: input.Count,
		}).Result()
		for _, message := range messages {
			result.MessageIDs = append(result.MessageIDs, message.ID)
		}
		result.Affected = int64(len(result.MessageIDs))
		result.NextStart = next
		return result, err
	case "xgroup-setid":
		if input.ExpectedCurrentGroupID == "" {
			return result, errors.New("expectedCurrentGroupId is required; prepare a new recovery plan")
		}
		raw, err := client.Eval(ctx, atomicGroupSetIDScript, []string{input.StreamKey},
			input.Group, input.ExpectedCurrentGroupID, input.TargetID).Result()
		if err != nil {
			return result, err
		}
		reply, ok := raw.([]interface{})
		if !ok || len(reply) != 2 {
			return result, errors.New("Redis returned an invalid XGROUP SETID script response")
		}
		status, err := strconv.ParseInt(fmt.Sprint(reply[0]), 10, 64)
		if err != nil {
			return result, errors.New("Redis returned an invalid XGROUP SETID script status")
		}
		currentID := fmt.Sprint(reply[1])
		switch status {
		case -1:
			return result, errors.New("consumer group was not found")
		case 0:
			return result, fmt.Errorf("%w: expected %s, found %s", errGroupRevisionChanged, input.ExpectedCurrentGroupID, currentID)
		case 1:
			result.Affected = 1
			result.TargetGroupID = input.TargetID
			return result, nil
		default:
			return result, errors.New("Redis returned an invalid XGROUP SETID script status")
		}
	default:
		return result, errors.New("unsupported recovery action")
	}
}

func recoveryExecutionFailure(err error) (int, string) {
	if errors.Is(err, errGroupRevisionChanged) {
		return http.StatusConflict, "recovery_plan_stale"
	}
	return http.StatusBadGateway, "redis_error"
}

func (s *apiServer) recoveryPlan(writer http.ResponseWriter, request *http.Request) {
	var input recoveryPlanRequest
	if err := readJSON(request, &input, recoveryRequestBodyLimit); err != nil {
		writeError(writer, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	input, err := normalizeRecoveryRequest(input)
	if err != nil {
		writeError(writer, http.StatusBadRequest, "invalid_recovery_plan", err.Error())
		return
	}
	session, _ := request.Context().Value(sessionContextKey).(sessionRecord)
	if !s.store.allowed(request.Context(), session, "groups:read", recoveryScope(input)) {
		writeError(writer, http.StatusForbidden, "permission_denied", "this stream is outside the permitted scope")
		return
	}
	connection, err := s.redis.get(input.ConnectionID)
	if err != nil {
		writeError(writer, http.StatusBadRequest, "unknown_connection", err.Error())
		return
	}
	plan, err := buildRecoveryPlan(request.Context(), connection.client, input)
	if err != nil {
		writeRedisError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, plan)
}

func (s *apiServer) executeRecovery(writer http.ResponseWriter, request *http.Request) {
	started := time.Now()
	var input recoveryExecutionRequest
	if err := readJSON(request, &input, recoveryRequestBodyLimit); err != nil {
		writeError(writer, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	normalized, err := normalizeRecoveryRequest(input.recoveryPlanRequest)
	if err != nil {
		writeError(writer, http.StatusBadRequest, "invalid_recovery_execution", err.Error())
		return
	}
	input.IdempotencyKey = strings.TrimSpace(input.IdempotencyKey)
	if err := validateIdempotencyKey(input.IdempotencyKey); err != nil {
		writeError(writer, http.StatusBadRequest, "invalid_idempotency_key", err.Error())
		return
	}
	session, _ := request.Context().Value(sessionContextKey).(sessionRecord)
	scope := recoveryScope(normalized)
	if !s.store.allowed(request.Context(), session, "groups:manage", scope) {
		s.auditRecovery(request, session, normalized.Action, scope, http.StatusForbidden, started, map[string]any{"reason": "permission_denied"})
		writeError(writer, http.StatusForbidden, "permission_denied", "this recovery action is outside the permitted stream scope")
		return
	}
	if normalized.Action == "xgroup-setid" && normalized.ExpectedCurrentGroupID == "" {
		s.auditRecovery(request, session, normalized.Action, scope, http.StatusBadRequest, started, map[string]any{"reason": "missing_group_revision"})
		writeError(writer, http.StatusBadRequest, "recovery_plan_required", "expectedCurrentGroupId is required; prepare a new recovery plan")
		return
	}
	if input.Confirmation != recoveryConfirmation(normalized) {
		s.auditRecovery(request, session, normalized.Action, scope, http.StatusBadRequest, started, map[string]any{"reason": "confirmation_mismatch"})
		writeError(writer, http.StatusBadRequest, "confirmation_required", "confirmation does not match the exact recovery plan")
		return
	}

	hash := recoveryRequestHash("recovery", normalized)
	stored, acquired, err := s.store.beginRecoveryExecution(request.Context(), input.IdempotencyKey, hash, session, normalized.Action, normalized.ConnectionID, normalized.StreamKey, normalized.Group)
	if err != nil {
		writeError(writer, http.StatusInternalServerError, "database_error", "unable to reserve the recovery execution")
		return
	}
	if !acquired {
		if stored.RequestHash != hash {
			s.auditRecovery(request, session, normalized.Action, scope, http.StatusConflict, started, map[string]any{"reason": "idempotency_conflict"})
			writeError(writer, http.StatusConflict, "idempotency_conflict", errIdempotencyConflict.Error())
			return
		}
		if s.writeStoredRecoveryExecution(writer, stored) {
			s.auditRecovery(request, session, normalized.Action, scope, stored.HTTPStatus, started, map[string]any{"idempotentReplay": true})
			return
		}
		writeError(writer, http.StatusConflict, "idempotency_conflict", errExecutionRunning.Error())
		return
	}

	connection, err := s.redis.get(normalized.ConnectionID)
	if err != nil {
		s.failRecoveryExecution(request.Context(), input.IdempotencyKey, http.StatusBadRequest, "unknown_connection", err.Error())
		s.auditRecovery(request, session, normalized.Action, scope, http.StatusBadRequest, started, map[string]any{"idempotencyKey": input.IdempotencyKey})
		writeError(writer, http.StatusBadRequest, "unknown_connection", err.Error())
		return
	}
	result, err := executeRecoveryCommand(request.Context(), connection.client, normalized)
	if err != nil {
		message := sanitizeRedisError(err.Error())
		status, code := recoveryExecutionFailure(err)
		details := map[string]any{"idempotencyKey": input.IdempotencyKey}
		if status == http.StatusConflict {
			details["reason"] = "group_revision_changed"
		}
		s.failRecoveryExecution(request.Context(), input.IdempotencyKey, status, code, message)
		s.auditRecovery(request, session, normalized.Action, scope, status, started, details)
		writeError(writer, status, code, message)
		return
	}
	responseJSON, _ := json.Marshal(result)
	if err := s.store.completeRecoveryExecution(request.Context(), input.IdempotencyKey, http.StatusOK, responseJSON); err != nil {
		// The Redis command may have succeeded while the SQLite marker failed.
		// The running marker deliberately blocks an automatic retry because
		// XCLAIM/XAUTOCLAIM do not share a transaction with SQLite. The operator
		// must inspect Redis and create a new plan before retrying.
		s.auditRecovery(request, session, normalized.Action, scope, http.StatusInternalServerError, started, map[string]any{"outcome": "redis_succeeded_sqlite_unknown"})
		writeError(writer, http.StatusInternalServerError, "execution_outcome_unknown", "Redis succeeded but the local execution result could not be persisted; inspect the group before retrying")
		return
	}
	s.auditRecovery(request, session, normalized.Action, scope, http.StatusOK, started, map[string]any{
		"idempotencyKey": input.IdempotencyKey, "affected": result.Affected,
	})
	writeJSON(writer, http.StatusOK, result)
}

func validateIdempotencyKey(value string) error {
	value = strings.TrimSpace(value)
	if len(value) < 8 || len(value) > 200 {
		return errors.New("idempotencyKey must be between 8 and 200 characters")
	}
	if strings.ContainsRune(value, '\x00') {
		return errors.New("idempotencyKey cannot contain a null character")
	}
	return nil
}

type storedRecoveryExecution struct {
	RequestHash  string
	Status       string
	HTTPStatus   int
	ResponseJSON []byte
	ErrorCode    string
	ErrorMessage string
}

func (s *store) beginRecoveryExecution(ctx context.Context, key, requestHash string, session sessionRecord, action, connectionID, streamKey, groupName string) (storedRecoveryExecution, bool, error) {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	result, err := s.db.ExecContext(ctx, `
		INSERT OR IGNORE INTO recovery_executions(
			idempotency_key,request_hash,user_id,username,action,connection_id,stream_key,group_name,status,created_at,updated_at
		) VALUES(?,?,?,?,?,?,?,?,?,?,?)`,
		key, requestHash, nullIfEmpty(session.UserID), session.Username, action, connectionID, streamKey, groupName, "running", now, now,
	)
	if err != nil {
		return storedRecoveryExecution{}, false, err
	}
	affected, _ := result.RowsAffected()
	if affected == 1 {
		return storedRecoveryExecution{RequestHash: requestHash, Status: "running"}, true, nil
	}
	stored, err := s.loadRecoveryExecution(ctx, key)
	return stored, false, err
}

func (s *store) loadRecoveryExecution(ctx context.Context, key string) (storedRecoveryExecution, error) {
	var result storedRecoveryExecution
	var response string
	err := s.db.QueryRowContext(ctx, `
		SELECT request_hash,status,http_status,response_json,error_code,error_message
		FROM recovery_executions WHERE idempotency_key=?`, key,
	).Scan(&result.RequestHash, &result.Status, &result.HTTPStatus, &response, &result.ErrorCode, &result.ErrorMessage)
	result.ResponseJSON = []byte(response)
	return result, err
}

func (s *store) completeRecoveryExecution(ctx context.Context, key string, status int, response []byte) error {
	result, err := s.db.ExecContext(ctx, `
		UPDATE recovery_executions
		SET status='succeeded',http_status=?,response_json=?,updated_at=?
		WHERE idempotency_key=? AND status='running'`,
		status, string(response), time.Now().UTC().Format(time.RFC3339Nano), key,
	)
	if err != nil {
		return err
	}
	affected, _ := result.RowsAffected()
	if affected != 1 {
		return errors.New("recovery execution is not running")
	}
	return nil
}

func (s *store) failRecoveryExecution(ctx context.Context, key string, status int, code, message string) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE recovery_executions
		SET status='failed',http_status=?,error_code=?,error_message=?,updated_at=?
		WHERE idempotency_key=? AND status='running'`,
		status, code, truncate(message, 512), time.Now().UTC().Format(time.RFC3339Nano), key,
	)
	return err
}

func (s *apiServer) failRecoveryExecution(ctx context.Context, key string, status int, code, message string) {
	_ = s.store.failRecoveryExecution(ctx, key, status, code, message)
}

func (s *apiServer) writeStoredRecoveryExecution(writer http.ResponseWriter, stored storedRecoveryExecution) bool {
	switch stored.Status {
	case "succeeded":
		var response recoveryExecutionResponse
		if json.Unmarshal(stored.ResponseJSON, &response) != nil {
			writeError(writer, http.StatusInternalServerError, "idempotency_record_invalid", "stored execution response is invalid")
			return true
		}
		response.IdempotentReplay = true
		writeJSON(writer, stored.HTTPStatus, response)
		return true
	case "failed":
		writeError(writer, stored.HTTPStatus, stored.ErrorCode, stored.ErrorMessage)
		return true
	default:
		return false
	}
}

func (s *apiServer) auditRecovery(request *http.Request, session sessionRecord, action, scope string, status int, started time.Time, details map[string]any) {
	item := makeAccessLog(request, session, "recovery:"+action, scope, status, time.Since(started), newRequestID())
	item.Details = details
	s.writeRequestAccessLog(item)
}

type quarantineRequest struct {
	ConnectionID   string   `json:"connectionId"`
	SourceStream   string   `json:"sourceStream"`
	SourceGroup    string   `json:"sourceGroup,omitempty"`
	DLQStream      string   `json:"dlqStream"`
	IDs            []string `json:"ids"`
	Acknowledge    bool     `json:"acknowledgeSource,omitempty"`
	Confirmation   string   `json:"confirmation,omitempty"`
	IdempotencyKey string   `json:"idempotencyKey,omitempty"`
}

type quarantinePlanResponse struct {
	ConnectionID       string   `json:"connectionId"`
	SourceStream       string   `json:"sourceStream"`
	SourceGroup        string   `json:"sourceGroup,omitempty"`
	DLQStream          string   `json:"dlqStream"`
	RequestedCount     int      `json:"requestedCount"`
	EntryCount         int      `json:"entryCount"`
	PayloadBytes       int      `json:"payloadBytes"`
	MissingIDs         []string `json:"missingIds"`
	AcknowledgeSource  bool     `json:"acknowledgeSource"`
	RequiredPermission []string `json:"requiredPermissions"`
	Confirmation       string   `json:"confirmation"`
	Guarantees         []string `json:"guarantees"`
}

type quarantineExecutionResponse struct {
	OK               bool               `json:"ok"`
	Items            []quarantineRecord `json:"items"`
	Acknowledged     int64              `json:"acknowledged"`
	IdempotentReplay bool               `json:"idempotentReplay,omitempty"`
}

type quarantineRecord struct {
	ID            int64          `json:"id"`
	ConnectionID  string         `json:"connectionId"`
	SourceStream  string         `json:"sourceStream"`
	SourceGroup   string         `json:"sourceGroup,omitempty"`
	SourceID      string         `json:"sourceId"`
	DLQStream     string         `json:"dlqStream"`
	DLQEntryID    string         `json:"dlqEntryId"`
	Fields        map[string]any `json:"fields"`
	Metadata      map[string]any `json:"metadata"`
	Status        string         `json:"status"`
	SourceAcked   bool           `json:"sourceAcknowledged"`
	ReplayTarget  string         `json:"replayTarget,omitempty"`
	ReplayEntryID string         `json:"replayEntryId,omitempty"`
	CreatedBy     string         `json:"createdBy,omitempty"`
	CreatedAt     string         `json:"createdAt"`
	UpdatedAt     string         `json:"updatedAt"`
	TerminalAt    string         `json:"terminalAt,omitempty"`
	fieldsJSON    string
	metadataJSON  string
}

type quarantineRedis interface {
	XRangeN(context.Context, string, string, string, int64) *redis.XMessageSliceCmd
	XAck(context.Context, string, string, ...string) *redis.IntCmd
	Eval(context.Context, string, []string, ...any) *redis.Cmd
}

type quarantineSourceEntry struct {
	ID         string
	Fields     map[string]any
	FieldsJSON string
	Bytes      int
}

type encodedRedisField struct {
	Name  string `json:"nameBase64"`
	Value string `json:"valueBase64"`
}

// encodeRedisFields does not assume JSON or UTF-8 payloads. Redis stream field
// names and values are byte strings, so both sides are base64 encoded before
// they are stored in SQLite or the DLQ envelope.
func encodeRedisFields(fields map[string]any) ([]byte, error) {
	if len(fields) == 0 || len(fields) > recoveryMaxFieldCount {
		return nil, fmt.Errorf("Redis entry must contain between 1 and %d fields", recoveryMaxFieldCount)
	}
	keys := make([]string, 0, len(fields))
	for key := range fields {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	encoded := make([]encodedRedisField, 0, len(keys))
	for _, key := range keys {
		var raw []byte
		switch value := fields[key].(type) {
		case []byte:
			raw = append([]byte(nil), value...)
		case string:
			raw = []byte(value)
		default:
			raw = []byte(fmt.Sprint(value))
		}
		encoded = append(encoded, encodedRedisField{
			Name: base64.StdEncoding.EncodeToString([]byte(key)), Value: base64.StdEncoding.EncodeToString(raw),
		})
	}
	return json.Marshal(encoded)
}

func decodeRedisFields(value string) (map[string]any, error) {
	var encoded []encodedRedisField
	if err := json.Unmarshal([]byte(value), &encoded); err != nil {
		return nil, err
	}
	fields := make(map[string]any, len(encoded))
	for _, item := range encoded {
		name, err := base64.StdEncoding.DecodeString(item.Name)
		if err != nil {
			return nil, errors.New("invalid encoded Redis field name")
		}
		raw, err := base64.StdEncoding.DecodeString(item.Value)
		if err != nil {
			return nil, errors.New("invalid encoded Redis field value")
		}
		fields[string(name)] = string(raw)
	}
	return fields, nil
}

func normalizeQuarantineRequest(input quarantineRequest, execution bool) (quarantineRequest, error) {
	input.ConnectionID = strings.TrimSpace(input.ConnectionID)
	input.SourceStream = strings.TrimSpace(input.SourceStream)
	input.SourceGroup = strings.TrimSpace(input.SourceGroup)
	input.DLQStream = strings.TrimSpace(input.DLQStream)
	input.IdempotencyKey = strings.TrimSpace(input.IdempotencyKey)
	if err := validateRecoveryIdentifier("connectionId", input.ConnectionID, 256); err != nil {
		return input, err
	}
	if err := validateRecoveryIdentifier("sourceStream", input.SourceStream, 1024); err != nil {
		return input, err
	}
	if err := validateRecoveryIdentifier("dlqStream", input.DLQStream, 1024); err != nil {
		return input, err
	}
	if input.SourceStream == input.DLQStream {
		return input, errors.New("dlqStream must differ from sourceStream")
	}
	if input.Acknowledge {
		if err := validateRecoveryIdentifier("sourceGroup", input.SourceGroup, 256); err != nil {
			return input, err
		}
	} else if input.SourceGroup != "" {
		if err := validateRecoveryIdentifier("sourceGroup", input.SourceGroup, 256); err != nil {
			return input, err
		}
	}
	seen := make(map[string]struct{}, len(input.IDs))
	ids := make([]string, 0, len(input.IDs))
	for _, rawID := range input.IDs {
		id := strings.TrimSpace(rawID)
		if err := validateStreamEntryID("id", id, false); err != nil {
			return input, err
		}
		if _, exists := seen[id]; exists {
			continue
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	if len(ids) == 0 || len(ids) > recoveryMaxCount {
		return input, fmt.Errorf("ids must contain between 1 and %d unique entries", recoveryMaxCount)
	}
	input.IDs = ids
	if execution {
		if err := validateIdempotencyKey(input.IdempotencyKey); err != nil {
			return input, err
		}
	}
	return input, nil
}

func quarantineCanonical(input quarantineRequest) any {
	return struct {
		ConnectionID string   `json:"connectionId"`
		SourceStream string   `json:"sourceStream"`
		SourceGroup  string   `json:"sourceGroup,omitempty"`
		DLQStream    string   `json:"dlqStream"`
		IDs          []string `json:"ids"`
		Acknowledge  bool     `json:"acknowledgeSource"`
	}{input.ConnectionID, input.SourceStream, input.SourceGroup, input.DLQStream, input.IDs, input.Acknowledge}
}

func quarantineConfirmation(input quarantineRequest) string {
	digest := recoveryRequestHash("quarantine", quarantineCanonical(input))
	return "confirm:quarantine:" + digest[:16]
}

func loadQuarantineSourceEntries(ctx context.Context, client quarantineRedis, stream string, ids []string) ([]quarantineSourceEntry, []string, int, error) {
	entries := make([]quarantineSourceEntry, 0, len(ids))
	missing := make([]string, 0)
	totalBytes := 0
	for _, id := range ids {
		messages, err := client.XRangeN(ctx, stream, id, id, 1).Result()
		if err != nil {
			return nil, nil, 0, err
		}
		if len(messages) == 0 || messages[0].ID != id {
			missing = append(missing, id)
			continue
		}
		fieldsJSON, err := encodeRedisFields(messages[0].Values)
		if err != nil {
			return nil, nil, 0, fmt.Errorf("encode source entry %s: %w", id, err)
		}
		if len(fieldsJSON) > recoveryMaxEntryPayloadBytes {
			return nil, nil, 0, fmt.Errorf("source entry %s exceeds the %d byte payload limit", id, recoveryMaxEntryPayloadBytes)
		}
		totalBytes += len(fieldsJSON)
		if totalBytes > recoveryMaxBatchPayloadBytes {
			return nil, nil, 0, fmt.Errorf("selected entries exceed the %d byte batch payload limit", recoveryMaxBatchPayloadBytes)
		}
		entries = append(entries, quarantineSourceEntry{ID: id, Fields: messages[0].Values, FieldsJSON: string(fieldsJSON), Bytes: len(fieldsJSON)})
	}
	return entries, missing, totalBytes, nil
}

func (s *apiServer) quarantinePlan(writer http.ResponseWriter, request *http.Request) {
	var input quarantineRequest
	if err := readJSON(request, &input, recoveryRequestBodyLimit); err != nil {
		writeError(writer, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	input, err := normalizeQuarantineRequest(input, false)
	if err != nil {
		writeError(writer, http.StatusBadRequest, "invalid_quarantine_plan", err.Error())
		return
	}
	session, _ := request.Context().Value(sessionContextKey).(sessionRecord)
	sourceScope := "stream:" + input.ConnectionID + ":" + input.SourceStream
	if !s.store.allowed(request.Context(), session, "streams:read", sourceScope) {
		writeError(writer, http.StatusForbidden, "permission_denied", "the source stream is outside the permitted scope")
		return
	}
	connection, err := s.redis.get(input.ConnectionID)
	if err != nil {
		writeError(writer, http.StatusBadRequest, "unknown_connection", err.Error())
		return
	}
	entries, missing, payloadBytes, err := loadQuarantineSourceEntries(request.Context(), connection.client, input.SourceStream, input.IDs)
	if err != nil {
		writeRedisError(writer, err)
		return
	}
	permissions := []string{"streams:write"}
	if input.Acknowledge {
		permissions = append(permissions, "groups:manage")
	}
	writeJSON(writer, http.StatusOK, quarantinePlanResponse{
		ConnectionID: input.ConnectionID, SourceStream: input.SourceStream, SourceGroup: input.SourceGroup,
		DLQStream: input.DLQStream, RequestedCount: len(input.IDs), EntryCount: len(entries), PayloadBytes: payloadBytes,
		MissingIDs: missing, AcknowledgeSource: input.Acknowledge, RequiredPermission: permissions,
		Confirmation: quarantineConfirmation(input), Guarantees: []string{
			"Dry-run only: no DLQ entry is written and no source pending entry is acknowledged.",
			"Execution writes the DLQ copy before an optional source XACK and never deletes the source entry.",
			"The source-to-DLQ boundary is at-least-once; a process failure can leave an unacknowledged source entry, never an acknowledged entry without a DLQ copy.",
		},
	})
}

func (s *apiServer) quarantineEntries(writer http.ResponseWriter, request *http.Request) {
	started := time.Now()
	var input quarantineRequest
	if err := readJSON(request, &input, recoveryRequestBodyLimit); err != nil {
		writeError(writer, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	input, err := normalizeQuarantineRequest(input, true)
	if err != nil {
		writeError(writer, http.StatusBadRequest, "invalid_quarantine_execution", err.Error())
		return
	}
	session, _ := request.Context().Value(sessionContextKey).(sessionRecord)
	sourceScope := "stream:" + input.ConnectionID + ":" + input.SourceStream
	dlqScope := "stream:" + input.ConnectionID + ":" + input.DLQStream
	if !s.store.allowed(request.Context(), session, "streams:write", sourceScope) ||
		!s.store.allowed(request.Context(), session, "streams:write", dlqScope) ||
		(input.Acknowledge && !s.store.allowed(request.Context(), session, "groups:manage", sourceScope)) {
		s.auditRecovery(request, session, "quarantine", sourceScope, http.StatusForbidden, started, map[string]any{"reason": "permission_denied"})
		writeError(writer, http.StatusForbidden, "permission_denied", "quarantine is outside the permitted source, DLQ, or group scope")
		return
	}
	if input.Confirmation != quarantineConfirmation(input) {
		writeError(writer, http.StatusBadRequest, "confirmation_required", "confirmation does not match the exact quarantine plan")
		return
	}
	canonical := quarantineCanonical(input)
	hash := recoveryRequestHash("quarantine", canonical)
	stored, acquired, err := s.store.beginRecoveryExecution(request.Context(), input.IdempotencyKey, hash, session, "quarantine", input.ConnectionID, input.SourceStream, input.SourceGroup)
	if err != nil {
		writeError(writer, http.StatusInternalServerError, "database_error", "unable to reserve the quarantine execution")
		return
	}
	if !acquired {
		if stored.RequestHash != hash {
			writeError(writer, http.StatusConflict, "idempotency_conflict", errIdempotencyConflict.Error())
			return
		}
		if stored.Status == "succeeded" {
			var response quarantineExecutionResponse
			if json.Unmarshal(stored.ResponseJSON, &response) != nil {
				writeError(writer, http.StatusInternalServerError, "idempotency_record_invalid", "stored execution response is invalid")
				return
			}
			response.IdempotentReplay = true
			writeJSON(writer, stored.HTTPStatus, response)
			return
		}
		if stored.Status == "failed" {
			writeError(writer, stored.HTTPStatus, stored.ErrorCode, stored.ErrorMessage)
			return
		}
		writeError(writer, http.StatusConflict, "execution_in_progress", errExecutionRunning.Error())
		return
	}

	connection, err := s.redis.get(input.ConnectionID)
	if err != nil {
		s.failRecoveryExecution(request.Context(), input.IdempotencyKey, http.StatusBadRequest, "unknown_connection", err.Error())
		writeError(writer, http.StatusBadRequest, "unknown_connection", err.Error())
		return
	}
	entries, missing, _, err := loadQuarantineSourceEntries(request.Context(), connection.client, input.SourceStream, input.IDs)
	if err != nil {
		message := sanitizeRedisError(err.Error())
		s.failRecoveryExecution(request.Context(), input.IdempotencyKey, http.StatusBadGateway, "redis_error", message)
		writeError(writer, http.StatusBadGateway, "redis_error", message)
		return
	}
	if len(missing) > 0 {
		message := "one or more source entries no longer exist: " + summarizeRecoveryIDs(missing, 10)
		s.failRecoveryExecution(request.Context(), input.IdempotencyKey, http.StatusConflict, "source_entries_missing", message)
		writeError(writer, http.StatusConflict, "source_entries_missing", message)
		return
	}

	items := make([]quarantineRecord, 0, len(entries))
	acknowledged := int64(0)
	for _, entry := range entries {
		record, err := s.store.loadQuarantineRecordByOrigin(
			request.Context(), input.ConnectionID, input.SourceStream, input.SourceGroup, entry.ID, input.DLQStream,
		)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			s.failRecoveryExecution(request.Context(), input.IdempotencyKey, http.StatusInternalServerError, "database_error", err.Error())
			writeError(writer, http.StatusInternalServerError, "database_error", "unable to inspect existing quarantine state")
			return
		}
		if errors.Is(err, sql.ErrNoRows) {
			metadata := map[string]any{
				"connectionId": input.ConnectionID, "sourceStream": input.SourceStream, "sourceGroup": input.SourceGroup,
				"sourceId": entry.ID, "quarantinedAt": time.Now().UTC().Format(time.RFC3339Nano), "quarantinedBy": session.Username,
			}
			metadataJSON, _ := json.Marshal(metadata)
			marker := redisSidecarKey(input.DLQStream, "quarantine", input.ConnectionID+"\x00"+input.SourceStream+"\x00"+input.SourceGroup+"\x00"+entry.ID)
			dlqID, _, addErr := redisXAddOnce(request.Context(), connection.client, input.DLQStream, marker, map[string]any{
				"_redisstreamscope_type": "quarantine", "source_connection": input.ConnectionID, "source_stream": input.SourceStream,
				"source_group": input.SourceGroup, "source_id": entry.ID, "fields_encoding": "base64-v1",
				"fields_json": entry.FieldsJSON, "metadata_json": string(metadataJSON),
			})
			if addErr != nil {
				message := sanitizeRedisError(addErr.Error())
				s.failRecoveryExecution(request.Context(), input.IdempotencyKey, http.StatusBadGateway, "redis_error", message)
				writeError(writer, http.StatusBadGateway, "redis_error", message)
				return
			}
			record, err = s.store.saveQuarantineRecord(request.Context(), quarantineRecord{
				ConnectionID: input.ConnectionID, SourceStream: input.SourceStream, SourceGroup: input.SourceGroup,
				SourceID: entry.ID, DLQStream: input.DLQStream, DLQEntryID: dlqID, Fields: entry.Fields, Metadata: metadata,
				Status: "quarantined", CreatedBy: session.Username, fieldsJSON: entry.FieldsJSON, metadataJSON: string(metadataJSON),
			})
			if err != nil {
				// DLQ XADD precedes SQLite and XACK. If SQLite is unavailable the
				// source remains pending; retry can observe the Redis sidecar marker
				// and reuse the same DLQ entry instead of losing the message.
				s.failRecoveryExecution(request.Context(), input.IdempotencyKey, http.StatusInternalServerError, "database_error", "DLQ was written but its local record could not be saved")
				writeError(writer, http.StatusInternalServerError, "quarantine_outcome_unknown", "DLQ was written but its local record could not be saved; the source was not acknowledged")
				return
			}
		}
		if input.Acknowledge {
			count, ackErr := connection.client.XAck(request.Context(), input.SourceStream, input.SourceGroup, entry.ID).Result()
			if ackErr != nil {
				message := sanitizeRedisError(ackErr.Error())
				s.failRecoveryExecution(request.Context(), input.IdempotencyKey, http.StatusBadGateway, "source_ack_failed", message)
				writeError(writer, http.StatusBadGateway, "source_ack_failed", message)
				return
			}
			if count > 0 {
				acknowledged += count
				if markErr := s.store.markQuarantineSourceAcked(request.Context(), record.ID); markErr != nil {
					s.failRecoveryExecution(request.Context(), input.IdempotencyKey, http.StatusInternalServerError, "database_error", markErr.Error())
					writeError(writer, http.StatusInternalServerError, "source_ack_outcome_unknown", "the source was acknowledged but its local quarantine state could not be updated")
					return
				}
				record.SourceAcked = true
			}
		}
		items = append(items, record)
	}
	response := quarantineExecutionResponse{OK: true, Items: items, Acknowledged: acknowledged}
	responseJSON, _ := json.Marshal(response)
	if err := s.store.completeRecoveryExecution(request.Context(), input.IdempotencyKey, http.StatusOK, responseJSON); err != nil {
		writeError(writer, http.StatusInternalServerError, "execution_outcome_unknown", "quarantine succeeded but the execution result could not be persisted")
		return
	}
	s.auditRecovery(request, session, "quarantine", sourceScope, http.StatusOK, started, map[string]any{
		"count": len(items), "acknowledged": acknowledged, "dlqStream": input.DLQStream,
	})
	writeJSON(writer, http.StatusOK, response)
}

func summarizeRecoveryIDs(ids []string, limit int) string {
	if len(ids) <= limit {
		return strings.Join(ids, ", ")
	}
	return strings.Join(ids[:limit], ", ") + fmt.Sprintf(" (+%d more)", len(ids)-limit)
}

const redisXAddOnceScript = `
local existing = redis.call('GET', KEYS[2])
if existing then
  redis.call('EXPIRE', KEYS[2], ARGV[1])
  return {existing, 0}
end
local values = {}
for index = 2, #ARGV do
  values[index - 1] = ARGV[index]
end
local entry = redis.call('XADD', KEYS[1], '*', unpack(values))
redis.call('SET', KEYS[2], entry, 'EX', ARGV[1])
return {entry, 1}
`

// redisXAddOnce makes replay/quarantine effectively-once while the Redis
// sidecar marker exists. SQLite remains the durable source of truth. Redis and
// SQLite cannot commit atomically, so the end-to-end guarantee is at-least-once:
// a crash may require inspection, but source acknowledgement never precedes the
// successful DLQ write.
func redisXAddOnce(ctx context.Context, client quarantineRedis, stream, marker string, fields map[string]any) (string, bool, error) {
	if len(fields) == 0 || len(fields) > recoveryMaxFieldCount {
		return "", false, fmt.Errorf("Redis entry must contain between 1 and %d fields", recoveryMaxFieldCount)
	}
	keys := make([]string, 0, len(fields))
	for key := range fields {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	arguments := make([]any, 0, 1+len(keys)*2)
	arguments = append(arguments, recoveryMarkerTTLSeconds)
	for _, key := range keys {
		arguments = append(arguments, key, fields[key])
	}
	value, err := client.Eval(ctx, redisXAddOnceScript, []string{stream, marker}, arguments...).Result()
	if err != nil {
		return "", false, err
	}
	parts, ok := value.([]any)
	if !ok || len(parts) != 2 {
		return "", false, errors.New("unexpected idempotent XADD response")
	}
	created, _ := strconv.ParseInt(fmt.Sprint(parts[1]), 10, 64)
	return fmt.Sprint(parts[0]), created == 1, nil
}

// redisSidecarKey always hashes to the same Redis Cluster slot as stream,
// including keys with unusual or empty hash tags. The sidecar is therefore
// safe to use with the two-key Lua script on standalone and cluster clients.
func redisSidecarKey(stream, kind, identity string) string {
	tag := redisHashTag(stream)
	if tag == "" {
		tag = redisTagForSlot(redisClusterSlot(stream))
	}
	digest := sha256.Sum256([]byte(identity))
	return "{" + tag + "}:redisstreamscope:" + kind + ":" + hex.EncodeToString(digest[:12])
}

func redisHashTag(key string) string {
	start := strings.IndexByte(key, '{')
	if start < 0 {
		return ""
	}
	endOffset := strings.IndexByte(key[start+1:], '}')
	if endOffset <= 0 {
		return ""
	}
	return key[start+1 : start+1+endOffset]
}

func redisClusterSlot(key string) uint16 {
	value := key
	if tag := redisHashTag(key); tag != "" {
		value = tag
	}
	return redisCRC16([]byte(value)) % 16384
}

func redisTagForSlot(slot uint16) string {
	if cached, exists := redisSlotTagCache.Load(slot); exists {
		return cached.(string)
	}
	for index := 0; ; index++ {
		candidate := "rss-" + strconv.Itoa(index)
		if redisCRC16([]byte(candidate))%16384 == slot {
			redisSlotTagCache.Store(slot, candidate)
			return candidate
		}
	}
}

func redisCRC16(value []byte) uint16 {
	var crc uint16
	for _, item := range value {
		crc ^= uint16(item) << 8
		for bit := 0; bit < 8; bit++ {
			if crc&0x8000 != 0 {
				crc = (crc << 1) ^ 0x1021
			} else {
				crc <<= 1
			}
		}
	}
	return crc
}

func (s *store) saveQuarantineRecord(ctx context.Context, item quarantineRecord) (quarantineRecord, error) {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err := s.db.ExecContext(ctx, `
		INSERT OR IGNORE INTO quarantine_records(
			connection_id,source_stream,source_group,source_id,dlq_stream,dlq_entry_id,fields_json,metadata_json,status,source_acked,created_by,created_at,updated_at
		) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		item.ConnectionID, item.SourceStream, item.SourceGroup, item.SourceID, item.DLQStream, item.DLQEntryID,
		item.fieldsJSON, item.metadataJSON, "quarantined", 0, item.CreatedBy, now, now,
	)
	if err != nil {
		return item, err
	}
	return s.loadQuarantineRecordByOrigin(ctx, item.ConnectionID, item.SourceStream, item.SourceGroup, item.SourceID, item.DLQStream)
}

func (s *store) loadQuarantineRecordByOrigin(ctx context.Context, connectionID, sourceStream, sourceGroup, sourceID, dlqStream string) (quarantineRecord, error) {
	row := s.db.QueryRowContext(ctx, quarantineRecordSelect+`
		WHERE connection_id=? AND source_stream=? AND source_group=? AND source_id=? AND dlq_stream=?`,
		connectionID, sourceStream, sourceGroup, sourceID, dlqStream,
	)
	return scanQuarantineRecord(row)
}

func (s *store) markQuarantineSourceAcked(ctx context.Context, id int64) error {
	_, err := s.db.ExecContext(ctx, `UPDATE quarantine_records SET source_acked=1,updated_at=? WHERE id=?`, time.Now().UTC().Format(time.RFC3339Nano), id)
	return err
}

const quarantineRecordSelect = `SELECT
	id,connection_id,source_stream,source_group,source_id,dlq_stream,dlq_entry_id,
	fields_json,metadata_json,status,source_acked,replay_target,replay_entry_id,
	created_by,created_at,updated_at,terminal_at
	FROM quarantine_records `

type rowScanner interface {
	Scan(...any) error
}

func scanQuarantineRecord(row rowScanner) (quarantineRecord, error) {
	var result quarantineRecord
	var sourceAcked int
	var replayTarget, replayEntryID, terminalAt sql.NullString
	err := row.Scan(
		&result.ID, &result.ConnectionID, &result.SourceStream, &result.SourceGroup, &result.SourceID,
		&result.DLQStream, &result.DLQEntryID, &result.fieldsJSON, &result.metadataJSON, &result.Status,
		&sourceAcked, &replayTarget, &replayEntryID, &result.CreatedBy, &result.CreatedAt, &result.UpdatedAt, &terminalAt,
	)
	if err != nil {
		return result, err
	}
	result.SourceAcked = sourceAcked == 1
	result.ReplayTarget = replayTarget.String
	result.ReplayEntryID = replayEntryID.String
	result.TerminalAt = terminalAt.String
	result.Fields, err = decodeRedisFields(result.fieldsJSON)
	if err != nil {
		return result, err
	}
	if err := json.Unmarshal([]byte(result.metadataJSON), &result.Metadata); err != nil {
		return result, err
	}
	return result, nil
}

type quarantinePage struct {
	Items      []quarantineRecord `json:"items"`
	NextCursor int64              `json:"nextCursor,omitempty"`
	HasMore    bool               `json:"hasMore"`
}

func (s *store) listQuarantineRecords(ctx context.Context, connectionID, sourceStream, status string, cursor int64, limit int) (quarantinePage, error) {
	if limit < 1 || limit > quarantineListMax {
		limit = 25
	}
	where := []string{"connection_id=?", "source_stream=?"}
	arguments := []any{connectionID, sourceStream}
	if status != "" && status != "all" {
		where = append(where, "status=?")
		arguments = append(arguments, status)
	}
	if cursor > 0 {
		where = append(where, "id<?")
		arguments = append(arguments, cursor)
	}
	arguments = append(arguments, limit+1)
	rows, err := s.db.QueryContext(ctx, quarantineRecordSelect+" WHERE "+strings.Join(where, " AND ")+" ORDER BY id DESC LIMIT ?", arguments...)
	if err != nil {
		return quarantinePage{}, err
	}
	defer rows.Close()
	items := make([]quarantineRecord, 0, limit+1)
	for rows.Next() {
		item, err := scanQuarantineRecord(rows)
		if err != nil {
			return quarantinePage{}, err
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return quarantinePage{}, err
	}
	page := quarantinePage{Items: items}
	if len(items) > limit {
		page.Items = items[:limit]
		page.HasMore = true
		page.NextCursor = page.Items[len(page.Items)-1].ID
	}
	return page, nil
}

func (s *apiServer) quarantineRecords(writer http.ResponseWriter, request *http.Request) {
	connectionID := strings.TrimSpace(request.URL.Query().Get("connectionId"))
	sourceStream := strings.TrimSpace(request.URL.Query().Get("key"))
	if err := validateRecoveryIdentifier("connectionId", connectionID, 256); err != nil {
		writeError(writer, http.StatusBadRequest, "invalid_connection", err.Error())
		return
	}
	if err := validateRecoveryIdentifier("key", sourceStream, 1024); err != nil {
		writeError(writer, http.StatusBadRequest, "invalid_stream", err.Error())
		return
	}
	status := strings.TrimSpace(request.URL.Query().Get("status"))
	if status != "" && status != "all" && status != "quarantined" && status != "replayed" && status != "skipped" {
		writeError(writer, http.StatusBadRequest, "invalid_status", "status must be all, quarantined, replayed or skipped")
		return
	}
	session, _ := request.Context().Value(sessionContextKey).(sessionRecord)
	if !s.store.allowed(request.Context(), session, "streams:read", "stream:"+connectionID+":"+sourceStream) {
		writeError(writer, http.StatusForbidden, "permission_denied", "the source stream is outside the permitted scope")
		return
	}
	page, err := s.store.listQuarantineRecords(
		request.Context(), connectionID, sourceStream, status,
		int64Query(request.URL.Query(), "cursor", 0, 0, 1<<62), int(int64Query(request.URL.Query(), "limit", 25, 1, quarantineListMax)),
	)
	if err != nil {
		writeError(writer, http.StatusInternalServerError, "database_error", "unable to list quarantine records")
		return
	}
	writeJSON(writer, http.StatusOK, page)
}

type quarantineActionRequest struct {
	Action         string  `json:"action"`
	ConnectionID   string  `json:"connectionId"`
	RecordIDs      []int64 `json:"recordIds"`
	TargetStream   string  `json:"targetStream,omitempty"`
	Confirmation   string  `json:"confirmation,omitempty"`
	IdempotencyKey string  `json:"idempotencyKey,omitempty"`
}

type quarantineActionPlanResponse struct {
	Action             string             `json:"action"`
	ConnectionID       string             `json:"connectionId"`
	RequestedCount     int                `json:"requestedCount"`
	EligibleCount      int                `json:"eligibleCount"`
	Records            []quarantineRecord `json:"records"`
	RequiredPermission string             `json:"requiredPermission"`
	Confirmation       string             `json:"confirmation"`
	Guarantees         []string           `json:"guarantees"`
}

type quarantineActionResponse struct {
	OK               bool               `json:"ok"`
	Action           string             `json:"action"`
	Items            []quarantineRecord `json:"items"`
	IdempotentReplay bool               `json:"idempotentReplay,omitempty"`
}

func normalizeQuarantineAction(input quarantineActionRequest, execution bool) (quarantineActionRequest, error) {
	input.Action = strings.ToLower(strings.TrimSpace(input.Action))
	input.ConnectionID = strings.TrimSpace(input.ConnectionID)
	input.TargetStream = strings.TrimSpace(input.TargetStream)
	input.IdempotencyKey = strings.TrimSpace(input.IdempotencyKey)
	if input.Action != "replay" && input.Action != "skip" {
		return input, errors.New("action must be replay or skip")
	}
	if err := validateRecoveryIdentifier("connectionId", input.ConnectionID, 256); err != nil {
		return input, err
	}
	if input.Action == "replay" && input.TargetStream != "" {
		if err := validateRecoveryIdentifier("targetStream", input.TargetStream, 1024); err != nil {
			return input, err
		}
	}
	seen := make(map[int64]struct{}, len(input.RecordIDs))
	ids := make([]int64, 0, len(input.RecordIDs))
	for _, id := range input.RecordIDs {
		if id <= 0 {
			return input, errors.New("recordIds must contain positive integers")
		}
		if _, exists := seen[id]; exists {
			continue
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	if len(ids) == 0 || len(ids) > recoveryMaxCount {
		return input, fmt.Errorf("recordIds must contain between 1 and %d unique records", recoveryMaxCount)
	}
	input.RecordIDs = ids
	if execution {
		if err := validateIdempotencyKey(input.IdempotencyKey); err != nil {
			return input, err
		}
	}
	return input, nil
}

func quarantineActionCanonical(input quarantineActionRequest) any {
	return struct {
		Action       string  `json:"action"`
		ConnectionID string  `json:"connectionId"`
		RecordIDs    []int64 `json:"recordIds"`
		TargetStream string  `json:"targetStream,omitempty"`
	}{input.Action, input.ConnectionID, input.RecordIDs, input.TargetStream}
}

func quarantineActionConfirmation(input quarantineActionRequest) string {
	digest := recoveryRequestHash("quarantine-action", quarantineActionCanonical(input))
	return "confirm:" + input.Action + ":" + digest[:16]
}

func (s *store) quarantineRecordsByID(ctx context.Context, connectionID string, ids []int64) ([]quarantineRecord, error) {
	placeholders := make([]string, len(ids))
	arguments := make([]any, 0, len(ids)+1)
	arguments = append(arguments, connectionID)
	for index, id := range ids {
		placeholders[index] = "?"
		arguments = append(arguments, id)
	}
	rows, err := s.db.QueryContext(ctx, quarantineRecordSelect+` WHERE connection_id=? AND id IN (`+strings.Join(placeholders, ",")+`)`, arguments...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	byID := make(map[int64]quarantineRecord, len(ids))
	for rows.Next() {
		item, err := scanQuarantineRecord(rows)
		if err != nil {
			return nil, err
		}
		byID[item.ID] = item
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	result := make([]quarantineRecord, 0, len(ids))
	for _, id := range ids {
		item, exists := byID[id]
		if !exists {
			return nil, fmt.Errorf("quarantine record %d was not found", id)
		}
		result = append(result, item)
	}
	return result, nil
}

func (s *apiServer) quarantineActionPlan(writer http.ResponseWriter, request *http.Request) {
	var input quarantineActionRequest
	if err := readJSON(request, &input, recoveryRequestBodyLimit); err != nil {
		writeError(writer, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	input, err := normalizeQuarantineAction(input, false)
	if err != nil {
		writeError(writer, http.StatusBadRequest, "invalid_quarantine_action", err.Error())
		return
	}
	records, err := s.store.quarantineRecordsByID(request.Context(), input.ConnectionID, input.RecordIDs)
	if err != nil {
		writeError(writer, http.StatusNotFound, "quarantine_record_not_found", err.Error())
		return
	}
	session, _ := request.Context().Value(sessionContextKey).(sessionRecord)
	for _, record := range records {
		if !s.store.allowed(request.Context(), session, "streams:read", "stream:"+input.ConnectionID+":"+record.SourceStream) {
			writeError(writer, http.StatusForbidden, "permission_denied", "a quarantine record is outside the permitted scope")
			return
		}
	}
	eligible := 0
	for _, record := range records {
		if record.Status == "quarantined" {
			eligible++
		}
	}
	guarantees := []string{"Only records still in quarantined state are changed; already replayed or skipped records remain unchanged."}
	if input.Action == "replay" {
		guarantees = append(guarantees,
			"Replay copies the original fields to the target stream. Completed replays are idempotent while the SQLite record exists; an ambiguous retry after Redis XADD is deduplicated by a 30-day Redis sidecar marker.",
		)
	} else {
		guarantees = append(guarantees, "Skip changes only local quarantine state and never deletes either the source or DLQ entry.")
	}
	writeJSON(writer, http.StatusOK, quarantineActionPlanResponse{
		Action: input.Action, ConnectionID: input.ConnectionID, RequestedCount: len(input.RecordIDs), EligibleCount: eligible,
		Records: records, RequiredPermission: "streams:write", Confirmation: quarantineActionConfirmation(input), Guarantees: guarantees,
	})
}

func (s *apiServer) executeQuarantineAction(writer http.ResponseWriter, request *http.Request) {
	started := time.Now()
	var input quarantineActionRequest
	if err := readJSON(request, &input, recoveryRequestBodyLimit); err != nil {
		writeError(writer, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	input, err := normalizeQuarantineAction(input, true)
	if err != nil {
		writeError(writer, http.StatusBadRequest, "invalid_quarantine_action", err.Error())
		return
	}
	if input.Confirmation != quarantineActionConfirmation(input) {
		writeError(writer, http.StatusBadRequest, "confirmation_required", "confirmation does not match the exact quarantine action plan")
		return
	}
	records, err := s.store.quarantineRecordsByID(request.Context(), input.ConnectionID, input.RecordIDs)
	if err != nil {
		writeError(writer, http.StatusNotFound, "quarantine_record_not_found", err.Error())
		return
	}
	session, _ := request.Context().Value(sessionContextKey).(sessionRecord)
	for _, record := range records {
		sourceScope := "stream:" + input.ConnectionID + ":" + record.SourceStream
		if !s.store.allowed(request.Context(), session, "streams:write", sourceScope) {
			writeError(writer, http.StatusForbidden, "permission_denied", "a quarantine record is outside the permitted source scope")
			return
		}
		target := input.TargetStream
		if target == "" {
			target = record.SourceStream
		}
		if input.Action == "replay" && !s.store.allowed(request.Context(), session, "streams:write", "stream:"+input.ConnectionID+":"+target) {
			writeError(writer, http.StatusForbidden, "permission_denied", "the replay target is outside the permitted scope")
			return
		}
	}

	canonical := quarantineActionCanonical(input)
	hash := recoveryRequestHash("quarantine-action", canonical)
	stored, acquired, err := s.store.beginRecoveryExecution(request.Context(), input.IdempotencyKey, hash, session, "quarantine-"+input.Action, input.ConnectionID, "", "")
	if err != nil {
		writeError(writer, http.StatusInternalServerError, "database_error", "unable to reserve the quarantine action")
		return
	}
	if !acquired {
		if stored.RequestHash != hash {
			writeError(writer, http.StatusConflict, "idempotency_conflict", errIdempotencyConflict.Error())
			return
		}
		if stored.Status == "succeeded" {
			var response quarantineActionResponse
			if json.Unmarshal(stored.ResponseJSON, &response) != nil {
				writeError(writer, http.StatusInternalServerError, "idempotency_record_invalid", "stored execution response is invalid")
				return
			}
			response.IdempotentReplay = true
			writeJSON(writer, stored.HTTPStatus, response)
			return
		}
		if stored.Status == "failed" {
			writeError(writer, stored.HTTPStatus, stored.ErrorCode, stored.ErrorMessage)
			return
		}
		writeError(writer, http.StatusConflict, "execution_in_progress", errExecutionRunning.Error())
		return
	}

	var connection *managedRedis
	if input.Action == "replay" {
		connection, err = s.redis.get(input.ConnectionID)
		if err != nil {
			s.failRecoveryExecution(request.Context(), input.IdempotencyKey, http.StatusBadRequest, "unknown_connection", err.Error())
			writeError(writer, http.StatusBadRequest, "unknown_connection", err.Error())
			return
		}
	}
	items := make([]quarantineRecord, 0, len(records))
	for _, record := range records {
		if record.Status != "quarantined" {
			items = append(items, record)
			continue
		}
		if input.Action == "skip" {
			updated, updateErr := s.store.markQuarantineSkipped(request.Context(), record.ID)
			if updateErr != nil {
				s.failRecoveryExecution(request.Context(), input.IdempotencyKey, http.StatusInternalServerError, "database_error", updateErr.Error())
				writeError(writer, http.StatusInternalServerError, "database_error", "unable to mark the quarantine record skipped")
				return
			}
			items = append(items, updated)
			continue
		}
		target := input.TargetStream
		if target == "" {
			target = record.SourceStream
		}
		marker := redisSidecarKey(target, "replay", input.ConnectionID+"\x00"+strconv.FormatInt(record.ID, 10)+"\x00"+target)
		entryID, _, replayErr := redisXAddOnce(request.Context(), connection.client, target, marker, record.Fields)
		if replayErr != nil {
			message := sanitizeRedisError(replayErr.Error())
			s.failRecoveryExecution(request.Context(), input.IdempotencyKey, http.StatusBadGateway, "redis_error", message)
			writeError(writer, http.StatusBadGateway, "redis_error", message)
			return
		}
		updated, updateErr := s.store.markQuarantineReplayed(request.Context(), record.ID, target, entryID)
		if updateErr != nil {
			// Redis may already contain the replay. A later plan can safely retry:
			// the sidecar marker returns the same entry ID rather than XADDing a
			// duplicate while it remains within its retention window.
			s.failRecoveryExecution(request.Context(), input.IdempotencyKey, http.StatusInternalServerError, "database_error", updateErr.Error())
			writeError(writer, http.StatusInternalServerError, "replay_outcome_unknown", "replay was written but its local state could not be persisted")
			return
		}
		items = append(items, updated)
	}
	response := quarantineActionResponse{OK: true, Action: input.Action, Items: items}
	responseJSON, _ := json.Marshal(response)
	if err := s.store.completeRecoveryExecution(request.Context(), input.IdempotencyKey, http.StatusOK, responseJSON); err != nil {
		writeError(writer, http.StatusInternalServerError, "execution_outcome_unknown", "the action succeeded but its execution result could not be persisted")
		return
	}
	s.auditRecovery(request, session, "quarantine-"+input.Action, "connection:"+input.ConnectionID, http.StatusOK, started, map[string]any{"count": len(items)})
	writeJSON(writer, http.StatusOK, response)
}

func (s *store) markQuarantineSkipped(ctx context.Context, id int64) (quarantineRecord, error) {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	result, err := s.db.ExecContext(ctx, `
		UPDATE quarantine_records SET status='skipped',updated_at=?,terminal_at=?
		WHERE id=? AND status='quarantined'`, now, now, id,
	)
	if err != nil {
		return quarantineRecord{}, err
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		return quarantineRecord{}, errors.New("quarantine record is no longer eligible")
	}
	return s.loadQuarantineRecordByID(ctx, id)
}

func (s *store) markQuarantineReplayed(ctx context.Context, id int64, target, entryID string) (quarantineRecord, error) {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	result, err := s.db.ExecContext(ctx, `
		UPDATE quarantine_records
		SET status='replayed',replay_target=?,replay_entry_id=?,updated_at=?,terminal_at=?
		WHERE id=? AND status='quarantined'`, target, entryID, now, now, id,
	)
	if err != nil {
		return quarantineRecord{}, err
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		return quarantineRecord{}, errors.New("quarantine record is no longer eligible")
	}
	return s.loadQuarantineRecordByID(ctx, id)
}

func (s *store) loadQuarantineRecordByID(ctx context.Context, id int64) (quarantineRecord, error) {
	return scanQuarantineRecord(s.db.QueryRowContext(ctx, quarantineRecordSelect+` WHERE id=?`, id))
}

// initRecoverySchema must be called from store.migrate after the core tables.
func initRecoverySchema(ctx context.Context, s *store) error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS recovery_executions (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			idempotency_key TEXT NOT NULL UNIQUE,
			request_hash TEXT NOT NULL,
			user_id TEXT,
			username TEXT NOT NULL DEFAULT '',
			action TEXT NOT NULL,
			connection_id TEXT NOT NULL,
			stream_key TEXT NOT NULL DEFAULT '',
			group_name TEXT NOT NULL DEFAULT '',
			status TEXT NOT NULL CHECK(status IN ('running','succeeded','failed')),
			http_status INTEGER NOT NULL DEFAULT 0,
			response_json TEXT NOT NULL DEFAULT '',
			error_code TEXT NOT NULL DEFAULT '',
			error_message TEXT NOT NULL DEFAULT '',
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS recovery_executions_created_idx ON recovery_executions(created_at DESC)`,
		`CREATE TABLE IF NOT EXISTS quarantine_records (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			connection_id TEXT NOT NULL,
			source_stream TEXT NOT NULL,
			source_group TEXT NOT NULL DEFAULT '',
			source_id TEXT NOT NULL,
			dlq_stream TEXT NOT NULL,
			dlq_entry_id TEXT NOT NULL,
			fields_json TEXT NOT NULL,
			metadata_json TEXT NOT NULL,
			status TEXT NOT NULL CHECK(status IN ('quarantined','replayed','skipped')),
			source_acked INTEGER NOT NULL DEFAULT 0,
			replay_target TEXT,
			replay_entry_id TEXT,
			created_by TEXT NOT NULL DEFAULT '',
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			terminal_at TEXT,
			UNIQUE(connection_id,source_stream,source_group,source_id,dlq_stream)
		)`,
		`CREATE INDEX IF NOT EXISTS quarantine_records_lookup_idx ON quarantine_records(connection_id,source_stream,status,id DESC)`,
		`CREATE INDEX IF NOT EXISTS quarantine_records_terminal_idx ON quarantine_records(terminal_at) WHERE terminal_at IS NOT NULL`,
	}
	for _, statement := range statements {
		if _, err := s.db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("recovery schema migration: %w", err)
		}
	}
	return nil
}

// pruneRecoveryHistory keeps active quarantine work indefinitely and removes
// only terminal records/execution receipts after 30 days. It is safe to call
// from the existing periodic retention pass.
func (s *store) pruneRecoveryHistory(ctx context.Context, now time.Time) error {
	cutoff := now.UTC().Add(-30 * 24 * time.Hour).Format(time.RFC3339Nano)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `DELETE FROM recovery_executions WHERE status!='running' AND updated_at<?`, cutoff); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM quarantine_records WHERE terminal_at IS NOT NULL AND terminal_at<?`, cutoff); err != nil {
		return err
	}
	return tx.Commit()
}
