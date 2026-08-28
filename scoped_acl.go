package main

import (
	"context"
	"fmt"
	"net/http"
	"strings"
)

type scopedPermissionGrant struct {
	action string
	scope  string
	effect string
}

type permissionGrantRows interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
	Close() error
}

type permissionGrantQueryFunc func(context.Context, string) (permissionGrantRows, error)

// scopedPermissionChecker loads a user's grants once and applies the exact
// same deny-wins semantics as store.allowed. Broad collection endpoints use it
// to filter every stream without issuing one SQLite query per returned row.
type scopedPermissionChecker struct {
	admin  bool
	role   string
	grants []scopedPermissionGrant
}

func (s *store) loadPermissionGrants(ctx context.Context, userID string) (grants []scopedPermissionGrant, err error) {
	query := s.permissionGrantQuery
	if query == nil {
		query = func(ctx context.Context, userID string) (permissionGrantRows, error) {
			return s.db.QueryContext(ctx, `SELECT action, scope, effect FROM user_grants WHERE user_id=?`, userID)
		}
	}
	rows, err := query(ctx, userID)
	if err != nil {
		return nil, fmt.Errorf("query permission grants: %w", err)
	}
	if rows == nil {
		return nil, fmt.Errorf("query permission grants: nil rows")
	}
	defer func() {
		if closeErr := rows.Close(); err == nil && closeErr != nil {
			err = fmt.Errorf("close permission grants: %w", closeErr)
			grants = nil
		}
	}()

	grants = make([]scopedPermissionGrant, 0)
	for rows.Next() {
		var grant scopedPermissionGrant
		if scanErr := rows.Scan(&grant.action, &grant.scope, &grant.effect); scanErr != nil {
			return nil, fmt.Errorf("scan permission grants: %w", scanErr)
		}
		grants = append(grants, grant)
	}
	if rowsErr := rows.Err(); rowsErr != nil {
		return nil, fmt.Errorf("iterate permission grants: %w", rowsErr)
	}
	return grants, nil
}

func (s *store) permissionChecker(ctx context.Context, session sessionRecord, action string) (scopedPermissionChecker, error) {
	checker := scopedPermissionChecker{
		admin:  session.Role == "admin",
		role:   session.Role,
		grants: []scopedPermissionGrant{},
	}
	if checker.admin {
		return checker, nil
	}
	grants, err := s.loadPermissionGrants(ctx, session.UserID)
	if err != nil {
		return scopedPermissionChecker{}, err
	}
	checker.grants = grants
	return checker, nil
}

func (checker scopedPermissionChecker) allows(action, scope string) bool {
	if checker.admin {
		return true
	}
	allowed := roleAllows(checker.role, action)
	for _, grant := range checker.grants {
		if !wildcardMatch(grant.action, action) || !wildcardMatch(grant.scope, scope) {
			continue
		}
		if grant.effect == "deny" {
			return false
		}
		if grant.effect == "allow" {
			allowed = true
		}
	}
	return allowed
}

// allowsScopeCoverage evaluates a scope that may describe more than one
// resource (for example stream:redis:*). A deny for any resource covered by
// the requested pattern wins, so aggregate records cannot reveal data from a
// narrower denied scope.
func (checker scopedPermissionChecker) allowsScopeCoverage(action, scopePattern string) bool {
	if checker.admin {
		return true
	}
	allowed := roleAllows(checker.role, action)
	for _, grant := range checker.grants {
		if !wildcardMatch(grant.action, action) {
			continue
		}
		if grant.effect == "deny" && scopePatternsOverlap(grant.scope, scopePattern) {
			return false
		}
		// An allow must cover the whole requested pattern. Merely overlapping a
		// broad collection is insufficient.
		if grant.effect == "allow" && wildcardMatch(grant.scope, scopePattern) {
			allowed = true
		}
	}
	return allowed
}

func scopePatternsOverlap(left, right string) bool {
	leftPrefix, leftWildcard := strings.CutSuffix(left, "*")
	rightPrefix, rightWildcard := strings.CutSuffix(right, "*")
	switch {
	case leftWildcard && rightWildcard:
		return strings.HasPrefix(leftPrefix, rightPrefix) || strings.HasPrefix(rightPrefix, leftPrefix)
	case leftWildcard:
		return strings.HasPrefix(right, leftPrefix)
	case rightWildcard:
		return strings.HasPrefix(left, rightPrefix)
	default:
		return left == right
	}
}

func redisStreamScope(connectionID, streamKey string) string {
	return "stream:" + connectionID + ":" + streamKey
}

func requestSession(request *http.Request) sessionRecord {
	session, _ := request.Context().Value(sessionContextKey).(sessionRecord)
	return session
}

func (s *apiServer) streamPermissionChecker(request *http.Request, action string) (scopedPermissionChecker, error) {
	return s.store.permissionChecker(request.Context(), requestSession(request), action)
}

func writePermissionCheckError(writer http.ResponseWriter) {
	writeError(writer, http.StatusInternalServerError, "permission_check_failed", "unable to evaluate permissions")
}

// requireNonAdminStreamScope prevents aggregation from combining authorized
// and denied streams before per-item filtering is possible. Admins retain the
// existing connection-wide and global query behavior.
func (s *apiServer) requireNonAdminStreamScope(writer http.ResponseWriter, request *http.Request, action, connectionID, streamKey, code string) bool {
	session := requestSession(request)
	connectionID = strings.TrimSpace(connectionID)
	streamKey = strings.TrimSpace(streamKey)
	if session.Role != "admin" && (connectionID == "" || streamKey == "") {
		writeError(writer, http.StatusBadRequest, code, "connectionId and streamKey are required")
		return false
	}
	if connectionID != "" && streamKey != "" {
		checker, err := s.store.permissionChecker(request.Context(), session, action)
		if err != nil {
			writePermissionCheckError(writer)
			return false
		}
		if !checker.allows(action, redisStreamScope(connectionID, streamKey)) {
			writeError(writer, http.StatusForbidden, "permission_denied", "You do not have permission to perform this action.")
			return false
		}
	}
	return true
}
