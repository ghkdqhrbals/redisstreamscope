package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

type userRecord struct {
	ID                     string     `json:"id"`
	Username               string     `json:"username"`
	DisplayName            string     `json:"displayName"`
	Role                   string     `json:"role"`
	Enabled                bool       `json:"enabled"`
	PasswordChangeRequired bool       `json:"passwordChangeRequired"`
	CreatedAt              time.Time  `json:"createdAt"`
	LastLoginAt            *time.Time `json:"lastLoginAt,omitempty"`
}

type sessionRecord struct {
	UserID                 string
	Username               string
	DisplayName            string
	Role                   string
	Enabled                bool
	PasswordChangeRequired bool
	ExpiresAt              time.Time
}

type accessLog struct {
	UserID    string
	Username  string
	Method    string
	Path      string
	Action    string
	Scope     string
	Status    int
	Duration  time.Duration
	IP        string
	UserAgent string
	RequestID string
	Details   map[string]any
}

type grantRecord struct {
	ID     int64  `json:"id"`
	UserID string `json:"userId"`
	Action string `json:"action"`
	Scope  string `json:"scope"`
	Effect string `json:"effect"`
}

type store struct {
	db *sql.DB
}

func openStore(config appConfig) (*store, error) {
	if err := os.MkdirAll(filepath.Dir(config.DataPath), 0o750); err != nil {
		return nil, fmt.Errorf("create data directory: %w", err)
	}
	dsn := "file:" + config.DataPath + "?_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(2)
	db.SetMaxIdleConns(1)
	db.SetConnMaxIdleTime(5 * time.Minute)
	result := &store{db: db}
	if err := result.migrate(context.Background()); err != nil {
		_ = db.Close()
		return nil, err
	}
	return result, nil
}

func (s *store) close() error {
	return s.db.Close()
}

func (s *store) migrate(ctx context.Context) error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS users (
			id TEXT PRIMARY KEY,
			username TEXT NOT NULL UNIQUE COLLATE NOCASE,
			display_name TEXT NOT NULL,
			password_hash TEXT NOT NULL,
			role TEXT NOT NULL CHECK(role IN ('viewer','operator','admin')),
			enabled INTEGER NOT NULL DEFAULT 1,
			must_change_password INTEGER NOT NULL DEFAULT 0,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			last_login_at TEXT
		)`,
		`CREATE TABLE IF NOT EXISTS sessions (
			id TEXT PRIMARY KEY,
			user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
			token_hash TEXT NOT NULL UNIQUE,
			created_at TEXT NOT NULL,
			expires_at TEXT NOT NULL,
			last_seen_at TEXT NOT NULL,
			ip TEXT NOT NULL,
			user_agent TEXT NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS sessions_expiry_idx ON sessions(expires_at)`,
		`CREATE TABLE IF NOT EXISTS user_grants (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
			action TEXT NOT NULL,
			scope TEXT NOT NULL,
			effect TEXT NOT NULL CHECK(effect IN ('allow','deny')),
			UNIQUE(user_id, action, scope)
		)`,
		`CREATE INDEX IF NOT EXISTS user_grants_user_idx ON user_grants(user_id)`,
		`CREATE TABLE IF NOT EXISTS access_logs (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			created_at TEXT NOT NULL,
			user_id TEXT,
			username TEXT NOT NULL,
			method TEXT NOT NULL,
			path TEXT NOT NULL,
			action TEXT NOT NULL,
			scope TEXT NOT NULL,
			status INTEGER NOT NULL,
			duration_ms INTEGER NOT NULL,
			ip TEXT NOT NULL,
			user_agent TEXT NOT NULL,
			request_id TEXT NOT NULL,
			details_json TEXT NOT NULL DEFAULT '{}'
		)`,
		`CREATE INDEX IF NOT EXISTS access_logs_created_idx ON access_logs(created_at DESC)`,
		`CREATE INDEX IF NOT EXISTS access_logs_user_idx ON access_logs(user_id, created_at DESC)`,
	}
	for _, statement := range statements {
		if _, err := s.db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("database migration: %w", err)
		}
	}
	if _, err := s.db.ExecContext(ctx, `ALTER TABLE users ADD COLUMN must_change_password INTEGER NOT NULL DEFAULT 0`); err != nil &&
		!strings.Contains(strings.ToLower(err.Error()), "duplicate column") {
		return fmt.Errorf("database migration: %w", err)
	}
	return nil
}

func (s *store) hasUsers(ctx context.Context) (bool, error) {
	var count int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM users`).Scan(&count); err != nil {
		return false, err
	}
	return count > 0, nil
}

func (s *store) createInitialAdmin(ctx context.Context, username, displayName, passwordHash string) (userRecord, error) {
	transaction, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return userRecord{}, err
	}
	defer transaction.Rollback()
	var count int
	if err := transaction.QueryRowContext(ctx, `SELECT COUNT(*) FROM users`).Scan(&count); err != nil {
		return userRecord{}, err
	}
	if count != 0 {
		return userRecord{}, errors.New("initial setup is already complete")
	}
	username = strings.TrimSpace(username)
	displayName = strings.TrimSpace(displayName)
	if displayName == "" {
		displayName = "System Administrator"
	}
	id, err := randomID(16)
	if err != nil {
		return userRecord{}, err
	}
	now := time.Now().UTC()
	if _, err = transaction.ExecContext(ctx, `INSERT INTO users(id, username, display_name, password_hash, role, enabled, must_change_password, created_at, updated_at) VALUES(?,?,?,?, 'admin', 1, 0, ?, ?)`,
		id, username, displayName, passwordHash, now.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano)); err != nil {
		return userRecord{}, err
	}
	if err := transaction.Commit(); err != nil {
		return userRecord{}, err
	}
	return userRecord{ID: id, Username: username, DisplayName: displayName, Role: "admin", Enabled: true, CreatedAt: now}, nil
}

func (s *store) authenticate(ctx context.Context, username string) (userRecord, string, error) {
	var user userRecord
	var enabled, mustChange int
	var created string
	var lastLogin sql.NullString
	var hash string
	err := s.db.QueryRowContext(ctx, `SELECT id, username, display_name, password_hash, role, enabled, must_change_password, created_at, last_login_at FROM users WHERE username = ?`, username).
		Scan(&user.ID, &user.Username, &user.DisplayName, &hash, &user.Role, &enabled, &mustChange, &created, &lastLogin)
	if err != nil {
		return user, "", err
	}
	user.Enabled = enabled == 1
	user.PasswordChangeRequired = mustChange == 1
	user.CreatedAt, _ = time.Parse(time.RFC3339Nano, created)
	if lastLogin.Valid {
		parsed, _ := time.Parse(time.RFC3339Nano, lastLogin.String)
		user.LastLoginAt = &parsed
	}
	return user, hash, nil
}

func (s *store) createSession(ctx context.Context, user userRecord, ttl time.Duration, ip, userAgent string) (string, time.Time, error) {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", time.Time{}, err
	}
	token := base64.RawURLEncoding.EncodeToString(raw)
	hash := sha256.Sum256([]byte(token))
	id, err := randomID(16)
	if err != nil {
		return "", time.Time{}, err
	}
	now := time.Now().UTC()
	expires := now.Add(ttl)
	transaction, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return "", time.Time{}, err
	}
	defer transaction.Rollback()
	if _, err = transaction.ExecContext(ctx, `DELETE FROM sessions WHERE expires_at <= ?`, now.Format(time.RFC3339Nano)); err != nil {
		return "", time.Time{}, err
	}
	if _, err = transaction.ExecContext(ctx, `INSERT INTO sessions(id, user_id, token_hash, created_at, expires_at, last_seen_at, ip, user_agent) VALUES(?,?,?,?,?,?,?,?)`,
		id, user.ID, hex.EncodeToString(hash[:]), now.Format(time.RFC3339Nano), expires.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano), ip, truncate(userAgent, 256)); err != nil {
		return "", time.Time{}, err
	}
	if _, err = transaction.ExecContext(ctx, `UPDATE users SET last_login_at = ?, updated_at = ? WHERE id = ?`, now.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano), user.ID); err != nil {
		return "", time.Time{}, err
	}
	if err = transaction.Commit(); err != nil {
		return "", time.Time{}, err
	}
	return token, expires, nil
}

func (s *store) session(ctx context.Context, token string) (sessionRecord, error) {
	hash := sha256.Sum256([]byte(token))
	var record sessionRecord
	var enabled, mustChange int
	var expires string
	err := s.db.QueryRowContext(ctx, `SELECT u.id, u.username, u.display_name, u.role, u.enabled, u.must_change_password, s.expires_at
		FROM sessions s JOIN users u ON u.id = s.user_id WHERE s.token_hash = ?`,
		hex.EncodeToString(hash[:])).Scan(&record.UserID, &record.Username, &record.DisplayName, &record.Role, &enabled, &mustChange, &expires)
	if err != nil {
		return record, err
	}
	record.Enabled = enabled == 1
	record.PasswordChangeRequired = mustChange == 1
	record.ExpiresAt, _ = time.Parse(time.RFC3339Nano, expires)
	if !record.Enabled || time.Now().After(record.ExpiresAt) {
		_, _ = s.db.ExecContext(ctx, `DELETE FROM sessions WHERE token_hash = ?`, hex.EncodeToString(hash[:]))
		return record, errors.New("session expired")
	}
	return record, nil
}

func (s *store) deleteSession(ctx context.Context, token string) {
	hash := sha256.Sum256([]byte(token))
	_, _ = s.db.ExecContext(ctx, `DELETE FROM sessions WHERE token_hash = ?`, hex.EncodeToString(hash[:]))
}

func (s *store) listUsers(ctx context.Context) ([]userRecord, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, username, display_name, role, enabled, must_change_password, created_at, last_login_at FROM users ORDER BY username`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	users := make([]userRecord, 0)
	for rows.Next() {
		var user userRecord
		var enabled, mustChange int
		var created string
		var last sql.NullString
		if err := rows.Scan(&user.ID, &user.Username, &user.DisplayName, &user.Role, &enabled, &mustChange, &created, &last); err != nil {
			return nil, err
		}
		user.Enabled = enabled == 1
		user.PasswordChangeRequired = mustChange == 1
		user.CreatedAt, _ = time.Parse(time.RFC3339Nano, created)
		if last.Valid {
			value, _ := time.Parse(time.RFC3339Nano, last.String)
			user.LastLoginAt = &value
		}
		users = append(users, user)
	}
	return users, rows.Err()
}

func (s *store) createUser(ctx context.Context, username, displayName, passwordHash, role string) (userRecord, error) {
	if role != "viewer" && role != "operator" && role != "admin" {
		return userRecord{}, errors.New("invalid role")
	}
	username = strings.TrimSpace(username)
	displayName = strings.TrimSpace(displayName)
	if len(username) < 3 || displayName == "" {
		return userRecord{}, errors.New("username and display name are required")
	}
	id, err := randomID(16)
	if err != nil {
		return userRecord{}, err
	}
	now := time.Now().UTC()
	_, err = s.db.ExecContext(ctx, `INSERT INTO users(id, username, display_name, password_hash, role, enabled, must_change_password, created_at, updated_at) VALUES(?,?,?,?,?,1,1,?,?)`,
		id, username, displayName, passwordHash, role, now.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano))
	if err != nil {
		return userRecord{}, err
	}
	return userRecord{ID: id, Username: username, DisplayName: displayName, Role: role, Enabled: true, PasswordChangeRequired: true, CreatedAt: now}, nil
}

func (s *store) updateUser(ctx context.Context, id, username, displayName, role string, enabled bool, passwordHash string) (userRecord, error) {
	if role != "viewer" && role != "operator" && role != "admin" {
		return userRecord{}, errors.New("invalid role")
	}
	username = strings.TrimSpace(username)
	displayName = strings.TrimSpace(displayName)
	if len(username) < 3 || displayName == "" {
		return userRecord{}, errors.New("username and display name are required")
	}
	transaction, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return userRecord{}, err
	}
	defer transaction.Rollback()
	var current userRecord
	var currentEnabled, mustChange int
	var created string
	var lastLogin sql.NullString
	if err := transaction.QueryRowContext(ctx, `SELECT id,username,display_name,role,enabled,must_change_password,created_at,last_login_at FROM users WHERE id=?`, id).
		Scan(&current.ID, &current.Username, &current.DisplayName, &current.Role, &currentEnabled, &mustChange, &created, &lastLogin); err != nil {
		return userRecord{}, err
	}
	current.Enabled = currentEnabled == 1
	if current.Role == "admin" && current.Enabled && (role != "admin" || !enabled) {
		var enabledAdmins int
		if err := transaction.QueryRowContext(ctx, `SELECT COUNT(*) FROM users WHERE role='admin' AND enabled=1`).Scan(&enabledAdmins); err != nil {
			return userRecord{}, err
		}
		if enabledAdmins <= 1 {
			return userRecord{}, errors.New("at least one enabled administrator is required")
		}
	}
	query := `UPDATE users SET username=?, display_name=?, role=?, enabled=?, updated_at=?`
	args := []any{username, displayName, role, boolInt(enabled), time.Now().UTC().Format(time.RFC3339Nano)}
	if passwordHash != "" {
		query += `, password_hash=?, must_change_password=1`
		args = append(args, passwordHash)
	}
	query += ` WHERE id=?`
	args = append(args, id)
	result, err := transaction.ExecContext(ctx, query, args...)
	if err != nil {
		return userRecord{}, err
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		return userRecord{}, sql.ErrNoRows
	}
	if !enabled || passwordHash != "" {
		if _, err := transaction.ExecContext(ctx, `DELETE FROM sessions WHERE user_id=?`, id); err != nil {
			return userRecord{}, err
		}
	}
	if err := transaction.Commit(); err != nil {
		return userRecord{}, err
	}
	current.Username = username
	current.DisplayName = displayName
	current.Role = role
	current.Enabled = enabled
	current.PasswordChangeRequired = passwordHash != "" || mustChange == 1
	current.CreatedAt, _ = time.Parse(time.RFC3339Nano, created)
	if lastLogin.Valid {
		value, _ := time.Parse(time.RFC3339Nano, lastLogin.String)
		current.LastLoginAt = &value
	}
	return current, nil
}

func (s *store) changeOwnPassword(ctx context.Context, userID, passwordHash string) error {
	transaction, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer transaction.Rollback()
	result, err := transaction.ExecContext(ctx, `UPDATE users SET password_hash=?, must_change_password=0, updated_at=? WHERE id=? AND enabled=1`,
		passwordHash, time.Now().UTC().Format(time.RFC3339Nano), userID)
	if err != nil {
		return err
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		return sql.ErrNoRows
	}
	if _, err = transaction.ExecContext(ctx, `DELETE FROM sessions WHERE user_id=?`, userID); err != nil {
		return err
	}
	return transaction.Commit()
}

func (s *store) changeOwnUsername(ctx context.Context, userID, username string) error {
	username = strings.TrimSpace(username)
	if len(username) < 3 {
		return errors.New("username must contain at least 3 characters")
	}
	result, err := s.db.ExecContext(ctx, `UPDATE users SET username=?, updated_at=? WHERE id=? AND enabled=1`,
		username, time.Now().UTC().Format(time.RFC3339Nano), userID)
	if err != nil {
		return err
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		return sql.ErrNoRows
	}
	return nil
}

func (s *store) listGrants(ctx context.Context, userID string) ([]grantRecord, error) {
	query := `SELECT id, user_id, action, scope, effect FROM user_grants`
	args := []any{}
	if userID != "" {
		query += ` WHERE user_id=?`
		args = append(args, userID)
	}
	query += ` ORDER BY user_id, action, scope`
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]grantRecord, 0)
	for rows.Next() {
		var grant grantRecord
		if err := rows.Scan(&grant.ID, &grant.UserID, &grant.Action, &grant.Scope, &grant.Effect); err != nil {
			return nil, err
		}
		result = append(result, grant)
	}
	return result, rows.Err()
}

func (s *store) upsertGrant(ctx context.Context, grant grantRecord) (grantRecord, error) {
	if grant.Effect != "allow" && grant.Effect != "deny" {
		return grant, errors.New("effect must be allow or deny")
	}
	if grant.UserID == "" || grant.Action == "" || grant.Scope == "" {
		return grant, errors.New("userId, action and scope are required")
	}
	result, err := s.db.ExecContext(ctx, `INSERT INTO user_grants(user_id,action,scope,effect) VALUES(?,?,?,?)
		ON CONFLICT(user_id,action,scope) DO UPDATE SET effect=excluded.effect`,
		grant.UserID, grant.Action, grant.Scope, grant.Effect)
	if err != nil {
		return grant, err
	}
	if _, err := result.RowsAffected(); err != nil {
		return grant, err
	}
	if err := s.db.QueryRowContext(ctx, `SELECT id FROM user_grants WHERE user_id=? AND action=? AND scope=?`, grant.UserID, grant.Action, grant.Scope).Scan(&grant.ID); err != nil {
		return grant, err
	}
	return grant, nil
}

func (s *store) deleteGrant(ctx context.Context, id int64) error {
	result, err := s.db.ExecContext(ctx, `DELETE FROM user_grants WHERE id=?`, id)
	if err != nil {
		return err
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		return sql.ErrNoRows
	}
	return nil
}

func (s *store) updateGrant(ctx context.Context, id int64, grant grantRecord) (grantRecord, error) {
	if grant.Effect != "allow" && grant.Effect != "deny" {
		return grant, errors.New("effect must be allow or deny")
	}
	if id <= 0 || grant.UserID == "" || grant.Action == "" || grant.Scope == "" {
		return grant, errors.New("id, userId, action and scope are required")
	}
	result, err := s.db.ExecContext(ctx, `UPDATE user_grants SET user_id=?,action=?,scope=?,effect=? WHERE id=?`,
		grant.UserID, grant.Action, grant.Scope, grant.Effect, id)
	if err != nil {
		return grant, err
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		return grant, sql.ErrNoRows
	}
	grant.ID = id
	return grant, nil
}

func (s *store) writeAccessLog(ctx context.Context, log accessLog) {
	details, _ := json.Marshal(log.Details)
	_, _ = s.db.ExecContext(ctx, `INSERT INTO access_logs(created_at,user_id,username,method,path,action,scope,status,duration_ms,ip,user_agent,request_id,details_json) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		time.Now().UTC().Format(time.RFC3339Nano), nullIfEmpty(log.UserID), log.Username, log.Method, truncate(log.Path, 512), log.Action, log.Scope, log.Status, log.Duration.Milliseconds(), log.IP, truncate(log.UserAgent, 256), log.RequestID, string(details))
	_, _ = s.db.ExecContext(ctx, `DELETE FROM access_logs WHERE id IN (SELECT id FROM access_logs ORDER BY id DESC LIMIT -1 OFFSET 100000)`)
}

func (s *store) listAccessLogs(ctx context.Context, limit int) ([]map[string]any, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id,created_at,username,method,path,action,scope,status,duration_ms,ip,request_id FROM access_logs ORDER BY id DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]map[string]any, 0, limit)
	for rows.Next() {
		var id, status, duration int64
		var created, username, method, path, action, scope, ip, requestID string
		if err := rows.Scan(&id, &created, &username, &method, &path, &action, &scope, &status, &duration, &ip, &requestID); err != nil {
			return nil, err
		}
		result = append(result, map[string]any{"id": id, "createdAt": created, "username": username, "method": method, "path": path, "action": action, "scope": scope, "status": status, "durationMs": duration, "ip": ip, "requestId": requestID})
	}
	return result, rows.Err()
}

func (s *store) allowed(ctx context.Context, session sessionRecord, action, scope string) bool {
	if session.Role == "admin" {
		return true
	}
	base := action == "profile:write"
	switch session.Role {
	case "viewer":
		base = base || action == "connections:read" || action == "streams:read" || action == "groups:read"
	case "operator":
		base = base || action == "connections:read" || action == "streams:read" || action == "groups:read" || action == "streams:write" || action == "groups:manage"
	}
	rows, err := s.db.QueryContext(ctx, `SELECT action, scope, effect FROM user_grants WHERE user_id=?`, session.UserID)
	if err != nil {
		return base
	}
	defer rows.Close()
	allowed := base
	for rows.Next() {
		var grantAction, grantScope, effect string
		if rows.Scan(&grantAction, &grantScope, &effect) != nil {
			continue
		}
		if wildcardMatch(grantAction, action) && wildcardMatch(grantScope, scope) {
			if effect == "deny" {
				return false
			}
			allowed = true
		}
	}
	return allowed
}

func randomID(bytes int) (string, error) {
	value := make([]byte, bytes)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(value), nil
}

func wildcardMatch(pattern, value string) bool {
	if pattern == "*" || pattern == value {
		return true
	}
	if strings.HasSuffix(pattern, "*") {
		return strings.HasPrefix(value, strings.TrimSuffix(pattern, "*"))
	}
	return false
}

func truncate(value string, max int) string {
	if len(value) <= max {
		return value
	}
	return value[:max]
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

func nullIfEmpty(value string) any {
	if value == "" {
		return nil
	}
	return value
}
