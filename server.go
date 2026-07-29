package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"golang.org/x/crypto/bcrypt"
)

type contextKey string

const sessionContextKey contextKey = "session"

type apiServer struct {
	config  appConfig
	store   *store
	redis   *redisManager
	auth    *authenticator
	tails   *tailBroker
	mux     *http.ServeMux
	spa     http.Handler
	started time.Time
}

type connectionInput struct {
	ID            string   `json:"id"`
	Name          string   `json:"name"`
	Mode          string   `json:"mode"`
	Addrs         []string `json:"addrs"`
	MasterName    string   `json:"masterName,omitempty"`
	Username      string   `json:"username,omitempty"`
	Password      string   `json:"password,omitempty"`
	PasswordSet   bool     `json:"passwordConfigured,omitempty"`
	ClearPassword bool     `json:"clearPassword,omitempty"`
	DB            int      `json:"db,omitempty"`
	TLS           bool     `json:"tls,omitempty"`
	TLSServer     string   `json:"tlsServerName,omitempty"`
	TLSCAFile     string   `json:"tlsCAFile,omitempty"`
	TLSCertFile   string   `json:"tlsCertFile,omitempty"`
	TLSKeyFile    string   `json:"tlsKeyFile,omitempty"`
	KeyPattern    string   `json:"keyPattern,omitempty"`
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (writer *statusWriter) WriteHeader(status int) {
	writer.status = status
	writer.ResponseWriter.WriteHeader(status)
}

func (writer *statusWriter) Flush() {
	if flusher, ok := writer.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func (writer *statusWriter) Unwrap() http.ResponseWriter {
	return writer.ResponseWriter
}

func newAPIServer(config appConfig, store *store, manager *redisManager, assets fs.FS) (*apiServer, error) {
	spa, err := serveSPA(webAssets)
	if err != nil {
		return nil, err
	}
	server := &apiServer{
		config: config, store: store, redis: manager,
		auth: newAuthenticator(config, store), tails: newTailBroker(config.MaxLiveStreams),
		mux: http.NewServeMux(), spa: spa, started: time.Now(),
	}
	server.routes()
	return server, nil
}

func (s *apiServer) routes() {
	s.mux.HandleFunc("GET /health/live", s.healthLive)
	s.mux.HandleFunc("GET /health/ready", s.healthReady)
	s.mux.HandleFunc("GET /api/setup/status", s.setupStatus)
	s.mux.HandleFunc("POST /api/setup/test-redis", s.setupTestRedis)
	s.mux.HandleFunc("POST /api/setup", s.setup)
	s.mux.HandleFunc("GET /api/session", s.session)
	s.mux.HandleFunc("POST /api/login", s.login)
	s.mux.HandleFunc("POST /api/logout", s.logout)
	s.mux.Handle("POST /api/me/password", s.protect("profile:write", s.changePassword))
	s.mux.Handle("POST /api/me/username", s.protect("profile:write", s.changeUsername))
	s.mux.Handle("GET /api/settings", s.protect("settings:read", s.settings))
	s.mux.Handle("PUT /api/settings", s.protect("settings:write", s.updateSettings))
	s.mux.Handle("POST /api/settings/test-redis", s.protect("settings:write", s.settingsTestRedis))

	s.mux.Handle("GET /api/connections", s.protect("connections:read", s.connections))
	s.mux.Handle("GET /api/overview", s.protect("streams:read", s.overview))
	s.mux.Handle("GET /api/metrics/timeseries", s.protect("streams:read", s.metricSeries))
	s.mux.Handle("GET /api/streams", s.protect("streams:read", s.streams))
	s.mux.Handle("GET /api/monitored-streams/status", s.protect("streams:read", s.monitoredStreamStatus))
	s.mux.Handle("POST /api/monitored-streams", s.protect("streams:write", s.addMonitoredStream))
	s.mux.Handle("DELETE /api/monitored-streams", s.protect("streams:write", s.deleteMonitoredStream))
	s.mux.Handle("GET /api/entries", s.protect("streams:read", s.entries))
	s.mux.Handle("GET /api/groups", s.protect("groups:read", s.groups))
	s.mux.Handle("GET /api/consumers", s.protect("groups:read", s.consumers))
	s.mux.Handle("GET /api/pending", s.protect("groups:read", s.pending))
	s.mux.Handle("GET /api/tail", s.protect("streams:read", s.tail))
	s.mux.Handle("POST /api/actions", s.protect("streams:read", s.action))

	s.mux.Handle("GET /api/users", s.protect("users:read", s.users))
	s.mux.Handle("POST /api/users", s.protect("users:write", s.createUser))
	s.mux.Handle("PATCH /api/users/{id}", s.protect("users:write", s.updateUser))
	s.mux.Handle("GET /api/access-logs", s.protect("access-logs:read", s.accessLogs))
	s.mux.Handle("GET /api/roles", s.protect("roles:read", s.roles))
	s.mux.Handle("GET /api/grants", s.protect("roles:read", s.grants))
	s.mux.Handle("PUT /api/grants", s.protect("roles:write", s.upsertGrant))
	s.mux.Handle("PATCH /api/grants/{id}", s.protect("roles:write", s.updateGrant))
	s.mux.Handle("DELETE /api/grants/{id}", s.protect("roles:write", s.deleteGrant))
}

func (s *apiServer) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	writer.Header().Set("X-Content-Type-Options", "nosniff")
	writer.Header().Set("X-Frame-Options", "DENY")
	writer.Header().Set("Referrer-Policy", "no-referrer")
	writer.Header().Set("Content-Security-Policy", "default-src 'self'; style-src 'self' https://fonts.googleapis.com; font-src https://fonts.gstatic.com; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; base-uri 'self'; form-action 'self'")
	if strings.HasPrefix(request.URL.Path, "/api/") || strings.HasPrefix(request.URL.Path, "/health/") {
		s.mux.ServeHTTP(writer, request)
		return
	}
	s.spa.ServeHTTP(writer, request)
}

func (s *apiServer) protect(action string, next http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		start := time.Now()
		requestID := newRequestID()
		writer.Header().Set("X-Request-ID", requestID)
		recorder := &statusWriter{ResponseWriter: writer, status: http.StatusOK}
		session, err := s.auth.session(request)
		if err != nil {
			writeError(recorder, http.StatusUnauthorized, "authentication_required", "로그인이 필요합니다.")
			return
		}
		scope := requestScope(request)
		if session.PasswordChangeRequired && request.URL.Path != "/api/me/password" {
			writeError(recorder, http.StatusForbidden, "password_change_required", "계속하려면 초기 비밀번호를 변경해야 합니다.")
			go s.store.writeAccessLog(context.Background(), makeAccessLog(request, session, action, scope, recorder.status, time.Since(start), requestID))
			return
		}
		if !s.store.allowed(request.Context(), session, action, scope) {
			writeError(recorder, http.StatusForbidden, "permission_denied", "이 작업을 수행할 권한이 없습니다.")
			go s.store.writeAccessLog(context.Background(), makeAccessLog(request, session, action, scope, recorder.status, time.Since(start), requestID))
			return
		}
		if request.Method != http.MethodGet && !validOrigin(request) {
			writeError(recorder, http.StatusForbidden, "invalid_origin", "요청 출처를 확인할 수 없습니다.")
			go s.store.writeAccessLog(context.Background(), makeAccessLog(request, session, action, scope, recorder.status, time.Since(start), requestID))
			return
		}
		request = request.WithContext(context.WithValue(request.Context(), sessionContextKey, session))
		next(recorder, request)
		go s.store.writeAccessLog(context.Background(), makeAccessLog(request, session, action, scope, recorder.status, time.Since(start), requestID))
	})
}

func (s *apiServer) healthLive(writer http.ResponseWriter, _ *http.Request) {
	writeJSON(writer, http.StatusOK, map[string]any{
		"status": "ok", "version": buildVersion,
		"uptimeSeconds": int(time.Since(s.started).Seconds()),
	})
}

func (s *apiServer) healthReady(writer http.ResponseWriter, request *http.Request) {
	ready := false
	for _, id := range s.redis.ids() {
		connection, _ := s.redis.get(id)
		ctx, cancel := context.WithTimeout(request.Context(), time.Second)
		err := connection.client.Ping(ctx).Err()
		cancel()
		if err == nil {
			ready = true
			break
		}
	}
	if !ready {
		writeJSON(writer, http.StatusServiceUnavailable, map[string]any{"status": "degraded"})
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"status": "ready"})
}

func (s *apiServer) setupStatus(writer http.ResponseWriter, request *http.Request) {
	configured, err := s.store.hasUsers(request.Context())
	if err != nil {
		writeError(writer, http.StatusInternalServerError, "database_error", "설정 상태를 확인하지 못했습니다.")
		return
	}
	connections := make([]map[string]any, 0, len(s.config.Connections))
	if !configured {
		for _, connection := range s.config.Connections {
			connections = append(connections, publicConnectionConfig(connection))
		}
	}
	writeJSON(writer, http.StatusOK, map[string]any{
		"setupRequired": !configured,
		"configPath":    s.config.ConfigPath,
		"connections":   connections,
	})
}

func (s *apiServer) setupTestRedis(writer http.ResponseWriter, request *http.Request) {
	configured, err := s.store.hasUsers(request.Context())
	if err != nil {
		writeError(writer, http.StatusInternalServerError, "database_error", "설정 상태를 확인하지 못했습니다.")
		return
	}
	if configured {
		writeError(writer, http.StatusForbidden, "setup_complete", "초기 설정이 이미 완료되었습니다.")
		return
	}
	if !validOrigin(request) {
		writeError(writer, http.StatusForbidden, "invalid_origin", "요청 출처를 확인할 수 없습니다.")
		return
	}
	s.testRedisInput(writer, request)
}

func (s *apiServer) setup(writer http.ResponseWriter, request *http.Request) {
	configured, err := s.store.hasUsers(request.Context())
	if err != nil {
		writeError(writer, http.StatusInternalServerError, "database_error", "설정 상태를 확인하지 못했습니다.")
		return
	}
	if configured {
		writeError(writer, http.StatusConflict, "setup_complete", "초기 설정이 이미 완료되었습니다.")
		return
	}
	if !validOrigin(request) {
		writeError(writer, http.StatusForbidden, "invalid_origin", "요청 출처를 확인할 수 없습니다.")
		return
	}
	var input struct {
		Admin struct {
			Username    string `json:"username"`
			DisplayName string `json:"displayName"`
			Password    string `json:"password"`
		} `json:"admin"`
		Connections []connectionInput `json:"connections"`
	}
	if err := readJSON(request, &input, 64<<10); err != nil {
		writeError(writer, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	if len(strings.TrimSpace(input.Admin.Username)) < 3 || len(input.Admin.Password) < 12 {
		writeError(writer, http.StatusBadRequest, "validation_failed", "관리자 이름은 3자, 비밀번호는 12자 이상이어야 합니다.")
		return
	}
	if len(input.Connections) == 0 {
		writeError(writer, http.StatusBadRequest, "validation_failed", "Redis 연결을 하나 이상 추가하세요.")
		return
	}
	connections, err := mergeConnectionInputs(input.Connections, s.config.Connections)
	if err != nil {
		writeError(writer, http.StatusBadRequest, "invalid_connections", err.Error())
		return
	}
	nextConfig := s.config
	nextConfig.Connections = connections
	if err := validateRuntimeConfig(nextConfig); err != nil {
		writeError(writer, http.StatusBadRequest, "invalid_connections", err.Error())
		return
	}
	passwordHash, err := hashPassword(input.Admin.Password)
	if err != nil {
		writeError(writer, http.StatusInternalServerError, "password_error", "관리자 비밀번호를 처리하지 못했습니다.")
		return
	}
	if err := savePropertiesConfig(s.config.ConfigPath, nextConfig); err != nil {
		writeError(writer, http.StatusInternalServerError, "config_write_failed", "config.properties를 저장하지 못했습니다.")
		return
	}
	if err := s.redis.reload(nextConfig); err != nil {
		writeError(writer, http.StatusInternalServerError, "redis_reload_failed", err.Error())
		return
	}
	s.tails.reset()
	user, err := s.store.createInitialAdmin(request.Context(), input.Admin.Username, input.Admin.DisplayName, passwordHash)
	if err != nil {
		writeError(writer, http.StatusConflict, "setup_failed", err.Error())
		return
	}
	token, expires, err := s.store.createSession(request.Context(), user, s.config.SessionTTL, requestIP(request), request.UserAgent())
	if err != nil {
		writeError(writer, http.StatusInternalServerError, "session_error", "관리자 세션을 만들지 못했습니다.")
		return
	}
	s.auth.setCookie(writer, token, expires)
	s.store.writeAccessLog(request.Context(), accessLog{
		UserID: user.ID, Username: user.Username, Method: request.Method, Path: request.URL.Path,
		Action: "setup:complete", Scope: "app", Status: http.StatusCreated, IP: requestIP(request),
		UserAgent: request.UserAgent(), RequestID: newRequestID(),
	})
	writeJSON(writer, http.StatusCreated, map[string]any{
		"authenticated": true, "username": user.Username, "displayName": user.DisplayName,
		"role": user.Role, "expiresAt": expires, "passwordChangeRequired": false,
	})
}

func (s *apiServer) session(writer http.ResponseWriter, request *http.Request) {
	record, err := s.auth.session(request)
	if err != nil {
		writeJSON(writer, http.StatusOK, map[string]any{"authenticated": false})
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{
		"authenticated": true, "username": record.Username, "displayName": record.DisplayName,
		"role": record.Role, "expiresAt": record.ExpiresAt, "passwordChangeRequired": record.PasswordChangeRequired,
	})
}

func (s *apiServer) login(writer http.ResponseWriter, request *http.Request) {
	if !s.auth.allowLogin(request) {
		writeError(writer, http.StatusTooManyRequests, "rate_limited", "로그인 시도가 너무 많습니다. 잠시 후 다시 시도하세요.")
		return
	}
	var input struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if err := readJSON(request, &input, 8<<10); err != nil {
		writeError(writer, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	user, token, expires, err := s.auth.login(request.Context(), strings.TrimSpace(input.Username), input.Password, requestIP(request), request.UserAgent())
	if err != nil {
		s.store.writeAccessLog(request.Context(), accessLog{Username: input.Username, Method: request.Method, Path: request.URL.Path, Action: "auth:login", Scope: "app", Status: http.StatusUnauthorized, IP: requestIP(request), UserAgent: request.UserAgent(), RequestID: newRequestID()})
		writeError(writer, http.StatusUnauthorized, "invalid_credentials", "사용자 이름 또는 비밀번호가 올바르지 않습니다.")
		return
	}
	s.auth.resetAttempts(request)
	s.auth.setCookie(writer, token, expires)
	s.store.writeAccessLog(request.Context(), accessLog{UserID: user.ID, Username: user.Username, Method: request.Method, Path: request.URL.Path, Action: "auth:login", Scope: "app", Status: http.StatusOK, IP: requestIP(request), UserAgent: request.UserAgent(), RequestID: newRequestID()})
	writeJSON(writer, http.StatusOK, map[string]any{
		"authenticated": true, "username": user.Username, "displayName": user.DisplayName,
		"role": user.Role, "expiresAt": expires, "passwordChangeRequired": user.PasswordChangeRequired,
	})
}

func (s *apiServer) logout(writer http.ResponseWriter, request *http.Request) {
	if !validOrigin(request) {
		writeError(writer, http.StatusForbidden, "invalid_origin", "요청 출처를 확인할 수 없습니다.")
		return
	}
	session, sessionErr := s.auth.session(request)
	s.auth.logout(request)
	s.auth.clearCookie(writer)
	if sessionErr == nil {
		s.store.writeAccessLog(request.Context(), makeAccessLog(request, session, "auth:logout", "app", http.StatusOK, 0, newRequestID()))
	}
	writeJSON(writer, http.StatusOK, map[string]any{"ok": true})
}

func (s *apiServer) changePassword(writer http.ResponseWriter, request *http.Request) {
	session, _ := request.Context().Value(sessionContextKey).(sessionRecord)
	var input struct {
		CurrentPassword string `json:"currentPassword"`
		NewPassword     string `json:"newPassword"`
	}
	if err := readJSON(request, &input, 8<<10); err != nil {
		writeError(writer, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	if len(input.NewPassword) < 12 {
		writeError(writer, http.StatusBadRequest, "validation_failed", "새 비밀번호는 12자 이상이어야 합니다.")
		return
	}
	if input.CurrentPassword == input.NewPassword {
		writeError(writer, http.StatusBadRequest, "validation_failed", "새 비밀번호는 현재 비밀번호와 달라야 합니다.")
		return
	}
	user, currentHash, err := s.store.authenticate(request.Context(), session.Username)
	if err != nil || bcrypt.CompareHashAndPassword([]byte(currentHash), []byte(input.CurrentPassword)) != nil {
		writeError(writer, http.StatusUnauthorized, "invalid_current_password", "현재 비밀번호가 올바르지 않습니다.")
		return
	}
	passwordHash, err := hashPassword(input.NewPassword)
	if err != nil {
		writeError(writer, http.StatusInternalServerError, "password_error", "비밀번호를 처리하지 못했습니다.")
		return
	}
	if err := s.store.changeOwnPassword(request.Context(), session.UserID, passwordHash); err != nil {
		writeError(writer, http.StatusInternalServerError, "password_update_failed", "비밀번호를 변경하지 못했습니다.")
		return
	}
	token, expires, err := s.store.createSession(request.Context(), user, s.config.SessionTTL, requestIP(request), request.UserAgent())
	if err != nil {
		writeError(writer, http.StatusInternalServerError, "session_error", "새 세션을 만들지 못했습니다. 다시 로그인하세요.")
		return
	}
	s.auth.setCookie(writer, token, expires)
	writeJSON(writer, http.StatusOK, map[string]any{
		"authenticated": true, "username": user.Username, "displayName": user.DisplayName,
		"role": user.Role, "expiresAt": expires, "passwordChangeRequired": false,
	})
}

func (s *apiServer) changeUsername(writer http.ResponseWriter, request *http.Request) {
	session, _ := request.Context().Value(sessionContextKey).(sessionRecord)
	var input struct {
		CurrentPassword string `json:"currentPassword"`
		Username        string `json:"username"`
	}
	if err := readJSON(request, &input, 8<<10); err != nil {
		writeError(writer, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	_, currentHash, err := s.store.authenticate(request.Context(), session.Username)
	if err != nil || bcrypt.CompareHashAndPassword([]byte(currentHash), []byte(input.CurrentPassword)) != nil {
		writeError(writer, http.StatusUnauthorized, "invalid_current_password", "현재 비밀번호가 올바르지 않습니다.")
		return
	}
	if err := s.store.changeOwnUsername(request.Context(), session.UserID, input.Username); err != nil {
		writeError(writer, http.StatusConflict, "username_update_failed", "사용자 이름을 변경하지 못했습니다.")
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"ok": true, "username": strings.TrimSpace(input.Username)})
}

func (s *apiServer) settings(writer http.ResponseWriter, request *http.Request) {
	current, _, err := readPropertiesConfig(s.config.ConfigPath)
	if err != nil {
		writeError(writer, http.StatusInternalServerError, "config_read_failed", err.Error())
		return
	}
	if err := validateConnections(current.Redis.Connections); err != nil {
		writeError(writer, http.StatusInternalServerError, "config_invalid", err.Error())
		return
	}
	items := make([]map[string]any, 0, len(current.Redis.Connections))
	for _, connection := range current.Redis.Connections {
		items = append(items, publicConnectionConfig(connection))
	}
	writeJSON(writer, http.StatusOK, map[string]any{
		"configPath":  s.config.ConfigPath,
		"connections": items,
	})
}

func (s *apiServer) updateSettings(writer http.ResponseWriter, request *http.Request) {
	var input struct {
		Connections []connectionInput `json:"connections"`
	}
	if err := readJSON(request, &input, 128<<10); err != nil {
		writeError(writer, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	currentDocument, _, err := readPropertiesConfig(s.config.ConfigPath)
	if err != nil {
		writeError(writer, http.StatusInternalServerError, "config_read_failed", err.Error())
		return
	}
	if err := validateConnections(currentDocument.Redis.Connections); err != nil {
		writeError(writer, http.StatusInternalServerError, "config_invalid", err.Error())
		return
	}
	connections, err := mergeConnectionInputs(input.Connections, currentDocument.Redis.Connections)
	if err != nil {
		writeError(writer, http.StatusBadRequest, "invalid_connections", err.Error())
		return
	}
	nextConfig := s.config
	nextConfig.Connections = connections
	if err := validateRuntimeConfig(nextConfig); err != nil {
		writeError(writer, http.StatusBadRequest, "invalid_connections", err.Error())
		return
	}
	if err := savePropertiesConfig(s.config.ConfigPath, nextConfig); err != nil {
		writeError(writer, http.StatusInternalServerError, "config_write_failed", "config.properties를 저장하지 못했습니다.")
		return
	}
	if err := s.redis.reload(nextConfig); err != nil {
		writeError(writer, http.StatusInternalServerError, "redis_reload_failed", err.Error())
		return
	}
	s.tails.reset()
	writeJSON(writer, http.StatusOK, map[string]any{"ok": true, "connections": len(connections)})
}

func (s *apiServer) settingsTestRedis(writer http.ResponseWriter, request *http.Request) {
	s.testRedisInput(writer, request)
}

func (s *apiServer) testRedisInput(writer http.ResponseWriter, request *http.Request) {
	var input connectionInput
	if err := readJSON(request, &input, 32<<10); err != nil {
		writeError(writer, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	connection, err := mergeConnectionInput(input, connectionConfig{})
	if err != nil {
		writeError(writer, http.StatusBadRequest, "invalid_connection", err.Error())
		return
	}
	if input.Password == "" && !input.ClearPassword {
		document, _, readErr := readPropertiesConfig(s.config.ConfigPath)
		if readErr == nil {
			readErr = validateConnections(document.Redis.Connections)
		}
		if readErr == nil {
			for _, existing := range document.Redis.Connections {
				if existing.ID == connection.ID {
					connection.Password = existing.Password
					connection.PasswordEnv = existing.PasswordEnv
					connection.PasswordFile = existing.PasswordFile
					break
				}
			}
		}
	}
	testConfig := s.config
	testConfig.Connections = []connectionConfig{connection}
	manager, err := newRedisManager(testConfig)
	if err != nil {
		writeError(writer, http.StatusBadRequest, "connection_failed", err.Error())
		return
	}
	defer manager.close()
	managed, err := manager.get(connection.ID)
	if err != nil {
		writeError(writer, http.StatusBadRequest, "connection_failed", err.Error())
		return
	}
	ctx, cancel := context.WithTimeout(request.Context(), 4*time.Second)
	defer cancel()
	start := time.Now()
	if err := managed.client.Ping(ctx).Err(); err != nil {
		writeError(writer, http.StatusBadGateway, "connection_failed", sanitizeRedisError(err.Error()))
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"ok": true, "latencyMs": float64(time.Since(start).Microseconds()) / 1000})
}

func (s *apiServer) connections(writer http.ResponseWriter, request *http.Request) {
	ids := s.redis.ids()
	result := make([]map[string]any, 0, len(ids))
	for _, id := range ids {
		connection, _ := s.redis.get(id)
		ctx, cancel := context.WithTimeout(request.Context(), 1500*time.Millisecond)
		start := time.Now()
		err := connection.client.Ping(ctx).Err()
		latency := time.Since(start)
		cancel()
		result = append(result, map[string]any{
			"id": connection.config.ID, "name": connection.config.Name, "mode": connection.config.Mode,
			"healthy": err == nil, "latencyMs": float64(latency.Microseconds()) / 1000,
			"tls": connection.config.TLS, "username": connection.config.Username,
		})
	}
	writeJSON(writer, http.StatusOK, map[string]any{"items": result})
}

func (s *apiServer) overview(writer http.ResponseWriter, request *http.Request) {
	connection, err := s.redis.get(request.URL.Query().Get("connectionId"))
	if err != nil {
		writeError(writer, http.StatusBadRequest, "unknown_connection", err.Error())
		return
	}
	ctx, cancel := context.WithTimeout(request.Context(), 10*time.Second)
	defer cancel()
	if err := connection.client.Ping(ctx).Err(); err != nil {
		writeRedisError(writer, err)
		return
	}
	monitored, err := s.store.listMonitoredStreams(ctx, connection.config.ID)
	if err != nil {
		writeError(writer, http.StatusInternalServerError, "monitored_streams_failed", "unable to load monitored streams")
		return
	}
	items, err := collectOverviewStreams(ctx, connection.client, connection.config.KeyPattern, s.config.MaxPageSize, monitored)
	if err != nil {
		writeRedisError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{
		"connectionId": connection.config.ID, "healthy": true, "items": items,
		"generatedAt": time.Now().UTC(),
	})
}

type overviewStream struct {
	Key            string `json:"key"`
	Length         int64  `json:"length"`
	ConsumerGroups int    `json:"consumerGroups"`
	TotalLag       int64  `json:"totalLag"`
	LagKnown       bool   `json:"lagKnown"`
	Pending        int64  `json:"pending"`
	LastConsumed   string `json:"lastConsumed"`
	Monitored      bool   `json:"monitored"`
	Available      bool   `json:"available"`
	RedisType      string `json:"redisType"`
}

type overviewPipelineClient interface {
	Pipelined(context.Context, func(redis.Pipeliner) error) ([]redis.Cmder, error)
}

func collectOverviewStreams(
	ctx context.Context,
	client redis.UniversalClient,
	pattern string,
	scanCount int64,
	monitored []monitoredStreamRecord,
) ([]overviewStream, error) {
	if pattern == "" {
		pattern = "*"
	}
	if scanCount < 1 {
		scanCount = 500
	}
	pipelineClient, canPipeline := client.(overviewPipelineClient)
	items := make([]overviewStream, 0)
	var cursor uint64
	for {
		result, err := client.Do(ctx, "SCAN", cursor, "MATCH", pattern, "COUNT", scanCount, "TYPE", "stream").Result()
		if err != nil {
			return nil, err
		}
		next, keys, err := parseScan(result)
		if err != nil {
			return nil, err
		}
		if len(keys) > 0 {
			batch, err := collectOverviewBatch(ctx, client, pipelineClient, canPipeline, keys)
			if err != nil {
				return nil, err
			}
			items = append(items, batch...)
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	indexByKey := make(map[string]int, len(items))
	for index := range items {
		indexByKey[items[index].Key] = index
	}
	for _, saved := range monitored {
		if index, exists := indexByKey[saved.Key]; exists {
			items[index].Monitored = true
			continue
		}
		redisType, err := client.Type(ctx, saved.Key).Result()
		if err != nil {
			return nil, err
		}
		if redisType == "stream" {
			batch, err := collectOverviewBatch(ctx, client, pipelineClient, canPipeline, []string{saved.Key})
			if err != nil {
				return nil, err
			}
			if len(batch) > 0 {
				batch[0].Monitored = true
				indexByKey[saved.Key] = len(items)
				items = append(items, batch[0])
				continue
			}
		}
		indexByKey[saved.Key] = len(items)
		items = append(items, overviewStream{
			Key: saved.Key, Monitored: true, Available: false, RedisType: redisType, LagKnown: true,
		})
	}
	sort.Slice(items, func(left, right int) bool {
		if items[left].Available != items[right].Available {
			return items[left].Available
		}
		if items[left].Monitored != items[right].Monitored {
			return items[left].Monitored
		}
		return items[left].Key < items[right].Key
	})
	return items, nil
}

func collectOverviewBatch(
	ctx context.Context,
	client redis.UniversalClient,
	pipelineClient overviewPipelineClient,
	canPipeline bool,
	keys []string,
) ([]overviewStream, error) {
	lengthCommands := make([]*redis.IntCmd, len(keys))
	groupCommands := make([]*redis.XInfoGroupsCmd, len(keys))
	if canPipeline {
		_, _ = pipelineClient.Pipelined(ctx, func(pipe redis.Pipeliner) error {
			for index, key := range keys {
				lengthCommands[index] = pipe.XLen(ctx, key)
				groupCommands[index] = pipe.XInfoGroups(ctx, key)
			}
			return nil
		})
	} else {
		for index, key := range keys {
			lengthCommands[index] = client.XLen(ctx, key)
			groupCommands[index] = client.XInfoGroups(ctx, key)
		}
	}

	items := make([]overviewStream, 0, len(keys))
	for index, key := range keys {
		length, err := lengthCommands[index].Result()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				continue
			}
			return nil, err
		}
		groups, err := groupCommands[index].Result()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				groups = nil
			} else {
				return nil, err
			}
		}
		groupCount, totalLag, lagKnown, pending, lastConsumed := summarizeOverviewGroups(groups)
		items = append(items, overviewStream{
			Key: key, Length: length, ConsumerGroups: groupCount,
			TotalLag: totalLag, LagKnown: lagKnown, Pending: pending, LastConsumed: lastConsumed,
			Available: true, RedisType: "stream",
		})
	}
	return items, nil
}

func summarizeOverviewGroups(groups []redis.XInfoGroup) (int, int64, bool, int64, string) {
	totalLag := int64(0)
	pending := int64(0)
	lagKnown := true
	lastConsumed := ""
	for _, group := range groups {
		pending += group.Pending
		if group.Lag < 0 {
			lagKnown = false
		} else {
			totalLag += group.Lag
		}
		if compareStreamIDs(group.LastDeliveredID, lastConsumed) > 0 {
			lastConsumed = group.LastDeliveredID
		}
	}
	return len(groups), totalLag, lagKnown, pending, lastConsumed
}

func compareStreamIDs(left, right string) int {
	if left == right {
		return 0
	}
	leftParts := strings.SplitN(left, "-", 2)
	rightParts := strings.SplitN(right, "-", 2)
	leftTime, leftTimeErr := strconv.ParseUint(leftParts[0], 10, 64)
	rightTime, rightTimeErr := strconv.ParseUint(rightParts[0], 10, 64)
	if leftTimeErr == nil && rightTimeErr == nil {
		if leftTime != rightTime {
			if leftTime < rightTime {
				return -1
			}
			return 1
		}
		leftSequence := uint64(0)
		rightSequence := uint64(0)
		if len(leftParts) > 1 {
			leftSequence, _ = strconv.ParseUint(leftParts[1], 10, 64)
		}
		if len(rightParts) > 1 {
			rightSequence, _ = strconv.ParseUint(rightParts[1], 10, 64)
		}
		if leftSequence < rightSequence {
			return -1
		}
		if leftSequence > rightSequence {
			return 1
		}
		return 0
	}
	return strings.Compare(left, right)
}

func (s *apiServer) streams(writer http.ResponseWriter, request *http.Request) {
	connection, err := s.redis.get(request.URL.Query().Get("connectionId"))
	if err != nil {
		writeError(writer, http.StatusBadRequest, "unknown_connection", err.Error())
		return
	}
	pattern := request.URL.Query().Get("pattern")
	if pattern == "" {
		pattern = connection.config.KeyPattern
	}
	cursor, _ := strconv.ParseUint(request.URL.Query().Get("cursor"), 10, 64)
	limit := int64Query(request.URL.Query(), "limit", 100, 1, s.config.MaxPageSize)
	result, err := connection.client.Do(request.Context(), "SCAN", cursor, "MATCH", pattern, "COUNT", limit, "TYPE", "stream").Result()
	if err != nil {
		writeRedisError(writer, err)
		return
	}
	next, keys, err := parseScan(result)
	if err != nil {
		writeError(writer, http.StatusBadGateway, "redis_response", err.Error())
		return
	}
	monitored, err := s.store.listMonitoredStreams(request.Context(), connection.config.ID)
	if err != nil {
		writeError(writer, http.StatusInternalServerError, "monitored_streams_failed", "unable to load monitored streams")
		return
	}
	monitoredKeys := make(map[string]struct{}, len(monitored))
	for _, item := range monitored {
		monitoredKeys[item.Key] = struct{}{}
	}
	if cursor == 0 {
		discovered := make(map[string]struct{}, len(keys))
		for _, key := range keys {
			discovered[key] = struct{}{}
		}
		for _, item := range monitored {
			if _, exists := discovered[item.Key]; !exists {
				keys = append(keys, item.Key)
			}
		}
	}
	items := make([]map[string]any, 0, len(keys))
	for _, key := range keys {
		_, isMonitored := monitoredKeys[key]
		redisType := "stream"
		available := true
		if isMonitored {
			redisType, err = connection.client.Type(request.Context(), key).Result()
			if err != nil {
				writeRedisError(writer, err)
				return
			}
			available = redisType == "stream"
		}
		if !available {
			items = append(items, map[string]any{
				"key": key, "length": int64(0), "monitored": true, "available": false, "redisType": redisType,
			})
			continue
		}
		length, lengthErr := connection.client.XLen(request.Context(), key).Result()
		if lengthErr == nil {
			items = append(items, map[string]any{
				"key": key, "length": length, "monitored": isMonitored, "available": true, "redisType": "stream",
			})
		}
	}
	sort.SliceStable(items, func(left, right int) bool {
		leftAvailable, _ := items[left]["available"].(bool)
		rightAvailable, _ := items[right]["available"].(bool)
		if leftAvailable != rightAvailable {
			return leftAvailable
		}
		leftMonitored, _ := items[left]["monitored"].(bool)
		rightMonitored, _ := items[right]["monitored"].(bool)
		if leftMonitored != rightMonitored {
			return leftMonitored
		}
		return items[left]["key"].(string) < items[right]["key"].(string)
	})
	writeJSON(writer, http.StatusOK, map[string]any{"items": items, "nextCursor": next, "hasMore": next != 0})
}

func (s *apiServer) addMonitoredStream(writer http.ResponseWriter, request *http.Request) {
	connection, err := s.redis.get(request.URL.Query().Get("connectionId"))
	if err != nil {
		writeError(writer, http.StatusBadRequest, "unknown_connection", err.Error())
		return
	}
	key, err := validateMonitoredStreamKey(request.URL.Query().Get("key"))
	if err != nil {
		writeError(writer, http.StatusBadRequest, "invalid_stream_key", err.Error())
		return
	}
	redisType, err := connection.client.Type(request.Context(), key).Result()
	if err != nil {
		writeRedisError(writer, err)
		return
	}
	if redisType != "none" && redisType != "stream" {
		writeError(writer, http.StatusConflict, "key_not_stream", fmt.Sprintf("%q is a Redis %s key, not a stream", key, redisType))
		return
	}
	session, _ := request.Context().Value(sessionContextKey).(sessionRecord)
	item, created, err := s.store.addMonitoredStream(request.Context(), connection.config.ID, key, session.UserID)
	if err != nil {
		writeError(writer, http.StatusInternalServerError, "monitor_stream_failed", "unable to save the monitored stream")
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	writeJSON(writer, status, map[string]any{
		"item": item, "created": created, "available": redisType == "stream", "redisType": redisType,
	})
}

func (s *apiServer) monitoredStreamStatus(writer http.ResponseWriter, request *http.Request) {
	connection, err := s.redis.get(request.URL.Query().Get("connectionId"))
	if err != nil {
		writeError(writer, http.StatusBadRequest, "unknown_connection", err.Error())
		return
	}
	key, err := validateMonitoredStreamKey(request.URL.Query().Get("key"))
	if err != nil {
		writeError(writer, http.StatusBadRequest, "invalid_stream_key", err.Error())
		return
	}
	redisType, err := connection.client.Type(request.Context(), key).Result()
	if err != nil {
		writeRedisError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{
		"key": key, "available": redisType == "stream", "exists": redisType != "none", "redisType": redisType,
	})
}

func (s *apiServer) deleteMonitoredStream(writer http.ResponseWriter, request *http.Request) {
	connection, err := s.redis.get(request.URL.Query().Get("connectionId"))
	if err != nil {
		writeError(writer, http.StatusBadRequest, "unknown_connection", err.Error())
		return
	}
	key, err := validateMonitoredStreamKey(request.URL.Query().Get("key"))
	if err != nil {
		writeError(writer, http.StatusBadRequest, "invalid_stream_key", err.Error())
		return
	}
	deleted, err := s.store.deleteMonitoredStream(request.Context(), connection.config.ID, key)
	if err != nil {
		writeError(writer, http.StatusInternalServerError, "unmonitor_stream_failed", "unable to remove the monitored stream")
		return
	}
	if !deleted {
		writeError(writer, http.StatusNotFound, "monitored_stream_not_found", "monitored stream was not found")
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"ok": true})
}

func validateMonitoredStreamKey(value string) (string, error) {
	key := strings.TrimSpace(value)
	if key == "" {
		return "", errors.New("stream key is required")
	}
	if len([]byte(key)) > 1024 {
		return "", errors.New("stream key must be 1024 bytes or fewer")
	}
	if strings.ContainsRune(key, '\x00') {
		return "", errors.New("stream key cannot contain a null character")
	}
	return key, nil
}

func (s *apiServer) entries(writer http.ResponseWriter, request *http.Request) {
	connection, key, ok := s.redisAndKey(writer, request)
	if !ok {
		return
	}
	limit := int64Query(request.URL.Query(), "limit", 50, 1, s.config.MaxPageSize)
	start := request.URL.Query().Get("start")
	end := request.URL.Query().Get("end")
	if start == "" {
		start = "+"
	}
	if end == "" {
		end = "-"
	}
	messages, err := connection.client.XRevRangeN(request.Context(), key, start, end, limit+1).Result()
	if err != nil {
		writeRedisError(writer, err)
		return
	}
	hasMore := int64(len(messages)) > limit
	if hasMore {
		messages = messages[:limit]
	}
	items := make([]map[string]any, 0, len(messages))
	for _, message := range messages {
		items = append(items, map[string]any{"id": message.ID, "fields": message.Values, "timestamp": streamIDTime(message.ID)})
	}
	next := ""
	if len(messages) > 0 {
		next = "(" + messages[len(messages)-1].ID
	}
	writeJSON(writer, http.StatusOK, map[string]any{"items": items, "nextCursor": next, "hasMore": hasMore})
}

func (s *apiServer) groups(writer http.ResponseWriter, request *http.Request) {
	connection, key, ok := s.redisAndKey(writer, request)
	if !ok {
		return
	}
	groups, err := connection.client.XInfoGroups(request.Context(), key).Result()
	if err != nil {
		writeRedisError(writer, err)
		return
	}
	items := make([]map[string]any, 0, len(groups))
	for _, group := range groups {
		items = append(items, map[string]any{
			"name": group.Name, "consumers": group.Consumers, "pending": group.Pending,
			"lastDeliveredId": group.LastDeliveredID, "entriesRead": group.EntriesRead, "lag": group.Lag,
		})
	}
	writeJSON(writer, http.StatusOK, map[string]any{"items": items})
}

func (s *apiServer) consumers(writer http.ResponseWriter, request *http.Request) {
	group := request.URL.Query().Get("group")
	if group == "" {
		writeError(writer, http.StatusBadRequest, "group_required", "group is required")
		return
	}
	connection, key, ok := s.redisAndKey(writer, request)
	if !ok {
		return
	}
	consumers, err := connection.client.XInfoConsumers(request.Context(), key, group).Result()
	if err != nil {
		writeRedisError(writer, err)
		return
	}
	items := make([]map[string]any, 0, len(consumers))
	for _, consumer := range consumers {
		items = append(items, map[string]any{
			"name": consumer.Name, "pending": consumer.Pending,
			"idleMs": consumer.Idle.Milliseconds(), "inactiveMs": consumer.Inactive.Milliseconds(),
		})
	}
	writeJSON(writer, http.StatusOK, map[string]any{"items": items})
}

func (s *apiServer) pending(writer http.ResponseWriter, request *http.Request) {
	connection, key, ok := s.redisAndKey(writer, request)
	if !ok {
		return
	}
	group := request.URL.Query().Get("group")
	if group == "" {
		writeError(writer, http.StatusBadRequest, "group_required", "group is required")
		return
	}
	count := int64Query(request.URL.Query(), "limit", 50, 1, 500)
	items, err := connection.client.XPendingExt(request.Context(), &redis.XPendingExtArgs{Stream: key, Group: group, Start: "-", End: "+", Count: count}).Result()
	if err != nil {
		writeRedisError(writer, err)
		return
	}
	result := make([]map[string]any, 0, len(items))
	for _, item := range items {
		result = append(result, map[string]any{
			"id": item.ID, "consumer": item.Consumer, "idleMs": item.Idle.Milliseconds(), "retryCount": item.RetryCount,
		})
	}
	writeJSON(writer, http.StatusOK, map[string]any{"items": result})
}

func (s *apiServer) tail(writer http.ResponseWriter, request *http.Request) {
	connection, key, ok := s.redisAndKey(writer, request)
	if !ok {
		return
	}
	lastID := tailStartID(request)
	subscription, err := s.tails.subscribe(connection, key, lastID)
	if err != nil {
		writeError(writer, http.StatusTooManyRequests, "tail_limit", err.Error())
		return
	}
	defer subscription.Cancel()
	writer.Header().Set("Content-Type", "text/event-stream")
	writer.Header().Set("Cache-Control", "no-cache, no-transform")
	writer.Header().Set("X-Accel-Buffering", "no")
	flusher, ok := writer.(http.Flusher)
	if !ok {
		writeError(writer, http.StatusInternalServerError, "stream_unsupported", "streaming is not supported")
		return
	}
	if err := http.NewResponseController(writer).SetWriteDeadline(time.Time{}); err != nil {
		writeError(writer, http.StatusInternalServerError, "stream_unsupported", "streaming write deadlines cannot be disabled")
		return
	}
	if _, err := io.WriteString(writer, "retry: 2000\n: connected\n\n"); err != nil {
		return
	}
	flusher.Flush()
	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()
	for {
		select {
		case event, open := <-subscription.Events:
			if !open {
				return
			}
			if _, err := fmt.Fprintf(writer, "id: %s\nevent: entry\ndata: %s\n\n", event.ID, event.Payload); err != nil {
				return
			}
			flusher.Flush()
		case <-heartbeat.C:
			if _, err := io.WriteString(writer, ": heartbeat\n\n"); err != nil {
				return
			}
			flusher.Flush()
		case <-request.Context().Done():
			return
		}
	}
}

func tailStartID(request *http.Request) string {
	if lastID := strings.TrimSpace(request.Header.Get("Last-Event-ID")); lastID != "" {
		return lastID
	}
	if lastID := strings.TrimSpace(request.URL.Query().Get("lastId")); lastID != "" {
		return lastID
	}
	return "$"
}

func (s *apiServer) action(writer http.ResponseWriter, request *http.Request) {
	var input struct {
		Action       string            `json:"action"`
		ConnectionID string            `json:"connectionId"`
		Key          string            `json:"key"`
		Group        string            `json:"group"`
		Consumer     string            `json:"consumer"`
		ID           string            `json:"id"`
		IDs          []string          `json:"ids"`
		Fields       map[string]string `json:"fields"`
		MaxLen       int64             `json:"maxLen"`
		MinIdleMs    int64             `json:"minIdleMs"`
		Start        string            `json:"start"`
		Count        int64             `json:"count"`
		Exact        bool              `json:"exact"`
		Confirm      string            `json:"confirm"`
	}
	if err := readJSON(request, &input, 64<<10); err != nil {
		writeError(writer, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	connection, err := s.redis.get(input.ConnectionID)
	if err != nil {
		writeError(writer, http.StatusBadRequest, "unknown_connection", err.Error())
		return
	}
	requiredPermission := "streams:write"
	if input.Action == "xack" || strings.HasPrefix(input.Action, "xgroup-") {
		requiredPermission = "groups:manage"
	}
	session, _ := request.Context().Value(sessionContextKey).(sessionRecord)
	exactScope := "stream:" + connection.config.ID + ":" + input.Key
	if !s.store.allowed(request.Context(), session, requiredPermission, exactScope) {
		s.store.writeAccessLog(request.Context(), makeAccessLog(request, session, input.Action, exactScope, http.StatusForbidden, 0, newRequestID()))
		writeError(writer, http.StatusForbidden, "permission_denied", "이 Redis resource에 대한 작업 권한이 없습니다.")
		return
	}
	var result any
	switch input.Action {
	case "xadd":
		result, err = connection.client.XAdd(request.Context(), newXAddArgs(input.Key, input.ID, input.Fields, input.MaxLen, input.Exact)).Result()
	case "xack":
		result, err = connection.client.XAck(request.Context(), input.Key, input.Group, input.IDs...).Result()
	case "xclaim":
		result, err = connection.client.XClaim(request.Context(), &redis.XClaimArgs{
			Stream: input.Key, Group: input.Group, Consumer: input.Consumer,
			MinIdle: time.Duration(input.MinIdleMs) * time.Millisecond, Messages: input.IDs,
		}).Result()
	case "xautoclaim":
		if input.Start == "" {
			input.Start = "0-0"
		}
		if input.Count <= 0 || input.Count > 500 {
			input.Count = 100
		}
		result, _, err = connection.client.XAutoClaim(request.Context(), &redis.XAutoClaimArgs{
			Stream: input.Key, Group: input.Group, Consumer: input.Consumer,
			MinIdle: time.Duration(input.MinIdleMs) * time.Millisecond, Start: input.Start, Count: input.Count,
		}).Result()
	case "xdel":
		if input.Confirm != input.Key {
			writeError(writer, http.StatusBadRequest, "confirmation_required", "stream key confirmation does not match")
			return
		}
		result, err = connection.client.XDel(request.Context(), input.Key, input.IDs...).Result()
	case "xtrim":
		if input.Confirm != input.Key || input.MaxLen < 0 {
			writeError(writer, http.StatusBadRequest, "confirmation_required", "stream key confirmation or maxLen is invalid")
			return
		}
		if input.Exact {
			result, err = connection.client.XTrimMaxLen(request.Context(), input.Key, input.MaxLen).Result()
		} else {
			result, err = connection.client.XTrimMaxLenApprox(request.Context(), input.Key, input.MaxLen, 0).Result()
		}
	case "xgroup-create":
		result, err = connection.client.XGroupCreateMkStream(request.Context(), input.Key, input.Group, input.ID).Result()
	case "xgroup-setid":
		result, err = connection.client.XGroupSetID(request.Context(), input.Key, input.Group, input.ID).Result()
	case "xgroup-create-consumer":
		result, err = connection.client.XGroupCreateConsumer(request.Context(), input.Key, input.Group, input.Consumer).Result()
	case "xgroup-delete-consumer":
		if input.Confirm != input.Consumer {
			writeError(writer, http.StatusBadRequest, "confirmation_required", "consumer confirmation does not match")
			return
		}
		result, err = connection.client.XGroupDelConsumer(request.Context(), input.Key, input.Group, input.Consumer).Result()
	case "xgroup-destroy":
		if input.Confirm != input.Key+"/"+input.Group {
			writeError(writer, http.StatusBadRequest, "confirmation_required", "stream/group confirmation does not match")
			return
		}
		result, err = connection.client.XGroupDestroy(request.Context(), input.Key, input.Group).Result()
	default:
		writeError(writer, http.StatusBadRequest, "unknown_action", "지원하지 않는 작업입니다.")
		return
	}
	if err != nil {
		s.store.writeAccessLog(request.Context(), makeAccessLog(request, session, input.Action, exactScope, http.StatusBadGateway, 0, newRequestID()))
		writeRedisError(writer, err)
		return
	}
	s.store.writeAccessLog(request.Context(), makeAccessLog(request, session, input.Action, exactScope, http.StatusOK, 0, newRequestID()))
	writeJSON(writer, http.StatusOK, map[string]any{"ok": true, "result": result})
}

func newXAddArgs(stream, id string, fields map[string]string, maxLen int64, exact bool) *redis.XAddArgs {
	return &redis.XAddArgs{
		Stream: stream, ID: id, Values: fields,
		MaxLen: maxLen, Approx: maxLen > 0 && !exact,
	}
}

func (s *apiServer) users(writer http.ResponseWriter, request *http.Request) {
	users, err := s.store.listUsers(request.Context())
	if err != nil {
		writeError(writer, http.StatusInternalServerError, "database_error", "사용자 목록을 불러오지 못했습니다.")
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"items": users})
}

func (s *apiServer) createUser(writer http.ResponseWriter, request *http.Request) {
	var input struct {
		Username    string `json:"username"`
		DisplayName string `json:"displayName"`
		Password    string `json:"password"`
		Role        string `json:"role"`
	}
	if err := readJSON(request, &input, 16<<10); err != nil {
		writeError(writer, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	if len(strings.TrimSpace(input.Username)) < 3 || strings.TrimSpace(input.DisplayName) == "" || len(input.Password) < 12 {
		writeError(writer, http.StatusBadRequest, "validation_failed", "사용자 이름은 3자 이상, 표시 이름은 필수이며 비밀번호는 12자 이상이어야 합니다.")
		return
	}
	hash, err := hashPassword(input.Password)
	if err != nil {
		writeError(writer, http.StatusInternalServerError, "password_error", "비밀번호를 처리하지 못했습니다.")
		return
	}
	user, err := s.store.createUser(request.Context(), input.Username, input.DisplayName, hash, input.Role)
	if err != nil {
		writeError(writer, http.StatusConflict, "user_exists", "사용자를 생성하지 못했습니다.")
		return
	}
	writeJSON(writer, http.StatusCreated, user)
}

func (s *apiServer) updateUser(writer http.ResponseWriter, request *http.Request) {
	var input struct {
		Username    string `json:"username"`
		DisplayName string `json:"displayName"`
		Role        string `json:"role"`
		Enabled     bool   `json:"enabled"`
		Password    string `json:"password,omitempty"`
	}
	if err := readJSON(request, &input, 8<<10); err != nil {
		writeError(writer, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	passwordHash := ""
	if input.Password != "" {
		if len(input.Password) < 12 {
			writeError(writer, http.StatusBadRequest, "validation_failed", "비밀번호는 12자 이상이어야 합니다.")
			return
		}
		var err error
		passwordHash, err = hashPassword(input.Password)
		if err != nil {
			writeError(writer, http.StatusInternalServerError, "password_error", "비밀번호를 처리하지 못했습니다.")
			return
		}
	}
	if len(strings.TrimSpace(input.Username)) < 3 || strings.TrimSpace(input.DisplayName) == "" {
		writeError(writer, http.StatusBadRequest, "validation_failed", "사용자 이름은 3자 이상이고 표시 이름은 비어 있지 않아야 합니다.")
		return
	}
	user, err := s.store.updateUser(request.Context(), request.PathValue("id"), input.Username, input.DisplayName, input.Role, input.Enabled, passwordHash)
	if err != nil {
		status := http.StatusBadRequest
		message := "사용자를 수정하지 못했습니다."
		if strings.Contains(err.Error(), "enabled administrator") {
			status = http.StatusConflict
			message = "활성화된 관리자는 최소 한 명 이상이어야 합니다."
		}
		writeError(writer, status, "update_failed", message)
		return
	}
	writeJSON(writer, http.StatusOK, user)
}

func (s *apiServer) accessLogs(writer http.ResponseWriter, request *http.Request) {
	limit := int(int64Query(request.URL.Query(), "limit", 100, 1, 500))
	items, err := s.store.listAccessLogs(request.Context(), limit)
	if err != nil {
		writeError(writer, http.StatusInternalServerError, "database_error", "접근 로그를 불러오지 못했습니다.")
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"items": items})
}

func (s *apiServer) roles(writer http.ResponseWriter, _ *http.Request) {
	writeJSON(writer, http.StatusOK, map[string]any{"items": []map[string]any{
		{"id": "viewer", "name": "Viewer", "permissions": []string{"connections:read", "streams:read", "groups:read"}},
		{"id": "operator", "name": "Operator", "permissions": []string{"connections:read", "streams:read", "streams:write", "groups:read", "groups:manage"}},
		{"id": "admin", "name": "Admin", "permissions": []string{"*"}},
	}})
}

func (s *apiServer) grants(writer http.ResponseWriter, request *http.Request) {
	items, err := s.store.listGrants(request.Context(), request.URL.Query().Get("userId"))
	if err != nil {
		writeError(writer, http.StatusInternalServerError, "database_error", "권한 목록을 불러오지 못했습니다.")
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"items": items})
}

func (s *apiServer) upsertGrant(writer http.ResponseWriter, request *http.Request) {
	var input grantRecord
	if err := readJSON(request, &input, 8<<10); err != nil {
		writeError(writer, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	grant, err := s.store.upsertGrant(request.Context(), input)
	if err != nil {
		writeError(writer, http.StatusBadRequest, "grant_failed", err.Error())
		return
	}
	writeJSON(writer, http.StatusOK, grant)
}

func (s *apiServer) deleteGrant(writer http.ResponseWriter, request *http.Request) {
	id, err := strconv.ParseInt(request.PathValue("id"), 10, 64)
	if err != nil || id <= 0 {
		writeError(writer, http.StatusBadRequest, "invalid_grant", "grant id가 올바르지 않습니다.")
		return
	}
	if err := s.store.deleteGrant(request.Context(), id); err != nil {
		writeError(writer, http.StatusNotFound, "grant_not_found", "권한을 찾을 수 없습니다.")
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"ok": true})
}

func (s *apiServer) updateGrant(writer http.ResponseWriter, request *http.Request) {
	id, err := strconv.ParseInt(request.PathValue("id"), 10, 64)
	if err != nil || id <= 0 {
		writeError(writer, http.StatusBadRequest, "invalid_grant", "grant id가 올바르지 않습니다.")
		return
	}
	var input grantRecord
	if err := readJSON(request, &input, 8<<10); err != nil {
		writeError(writer, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	grant, err := s.store.updateGrant(request.Context(), id, input)
	if err != nil {
		writeError(writer, http.StatusBadRequest, "grant_update_failed", "권한을 변경하지 못했습니다.")
		return
	}
	writeJSON(writer, http.StatusOK, grant)
}

func (s *apiServer) redisAndKey(writer http.ResponseWriter, request *http.Request) (*managedRedis, string, bool) {
	connection, err := s.redis.get(request.URL.Query().Get("connectionId"))
	if err != nil {
		writeError(writer, http.StatusBadRequest, "unknown_connection", err.Error())
		return nil, "", false
	}
	key := request.URL.Query().Get("key")
	if key == "" {
		writeError(writer, http.StatusBadRequest, "key_required", "key is required")
		return nil, "", false
	}
	return connection, key, true
}

func readJSON(request *http.Request, target any, max int64) error {
	body, err := io.ReadAll(io.LimitReader(request.Body, max+1))
	if err != nil {
		return err
	}
	if int64(len(body)) > max {
		return errors.New("request body is too large")
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return errors.New("request body must contain one JSON object")
	}
	return nil
}

func writeJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(value)
}

func writeError(writer http.ResponseWriter, status int, code, message string) {
	writeJSON(writer, status, map[string]any{"error": message, "code": code})
}

func writeRedisError(writer http.ResponseWriter, err error) {
	status := http.StatusBadGateway
	code := "redis_error"
	if errors.Is(err, redis.Nil) {
		status, code = http.StatusNotFound, "not_found"
	}
	writeError(writer, status, code, sanitizeRedisError(err.Error()))
}

func parseScan(value any) (uint64, []string, error) {
	parts, ok := value.([]any)
	if !ok || len(parts) != 2 {
		return 0, nil, errors.New("unexpected SCAN response")
	}
	cursor, err := strconv.ParseUint(fmt.Sprint(parts[0]), 10, 64)
	if err != nil {
		return 0, nil, err
	}
	rawKeys, ok := parts[1].([]any)
	if !ok {
		return cursor, nil, nil
	}
	keys := make([]string, 0, len(rawKeys))
	for _, key := range rawKeys {
		keys = append(keys, fmt.Sprint(key))
	}
	return cursor, keys, nil
}

func int64Query(values url.Values, key string, fallback, minimum, maximum int64) int64 {
	value, err := strconv.ParseInt(values.Get(key), 10, 64)
	if err != nil || value < minimum {
		return fallback
	}
	if value > maximum {
		return maximum
	}
	return value
}

func streamIDTime(id string) time.Time {
	millis, _ := strconv.ParseInt(strings.SplitN(id, "-", 2)[0], 10, 64)
	return time.UnixMilli(millis).UTC()
}

func requestScope(request *http.Request) string {
	connection := request.URL.Query().Get("connectionId")
	key := request.URL.Query().Get("key")
	if connection == "" {
		connection = "*"
	}
	if key != "" {
		return "stream:" + connection + ":" + key
	}
	if strings.HasPrefix(request.URL.Path, "/api/users") {
		if id := request.PathValue("id"); id != "" {
			return "user:" + id
		}
		return "users:*"
	}
	if strings.HasPrefix(request.URL.Path, "/api/grants") {
		if id := request.PathValue("id"); id != "" {
			return "grant:" + id
		}
		return "grants:*"
	}
	if strings.HasPrefix(request.URL.Path, "/api/access-logs") {
		return "access-logs:*"
	}
	if strings.HasPrefix(request.URL.Path, "/api/me/") {
		return "profile:self"
	}
	if strings.HasPrefix(request.URL.Path, "/api/settings") {
		return "settings:*"
	}
	return "connection:" + connection
}

func validOrigin(request *http.Request) bool {
	origin := request.Header.Get("Origin")
	if origin == "" {
		return true
	}
	parsed, err := url.Parse(origin)
	return err == nil && parsed.Host == request.Host
}

func sanitizeRedisError(message string) string {
	if len(message) > 240 {
		message = message[:240]
	}
	return message
}

func newRequestID() string {
	value := make([]byte, 8)
	_, _ = rand.Read(value)
	return hex.EncodeToString(value)
}

func makeAccessLog(request *http.Request, session sessionRecord, action, scope string, status int, duration time.Duration, requestID string) accessLog {
	return accessLog{
		UserID: session.UserID, Username: session.Username, Method: request.Method, Path: request.URL.Path,
		Action: action, Scope: scope, Status: status, Duration: duration, IP: requestIP(request),
		UserAgent: request.UserAgent(), RequestID: requestID,
	}
}

func hashPassword(password string) (string, error) {
	hash, err := bcrypt.GenerateFromPassword([]byte(password), 12)
	return string(hash), err
}

func mergeConnectionInputs(inputs []connectionInput, existing []connectionConfig) ([]connectionConfig, error) {
	byID := make(map[string]connectionConfig, len(existing))
	for _, connection := range existing {
		byID[connection.ID] = connection
	}
	result := make([]connectionConfig, 0, len(inputs))
	for _, input := range inputs {
		connection, err := mergeConnectionInput(input, byID[input.ID])
		if err != nil {
			return nil, err
		}
		result = append(result, connection)
	}
	if err := validateConnections(result); err != nil {
		return nil, err
	}
	return result, nil
}

func mergeConnectionInput(input connectionInput, existing connectionConfig) (connectionConfig, error) {
	password := input.Password
	passwordEnv := ""
	passwordFile := ""
	if password == "" && !input.ClearPassword {
		password = existing.Password
		passwordEnv = existing.PasswordEnv
		passwordFile = existing.PasswordFile
	}
	connection := connectionConfig{
		ID: input.ID, Name: input.Name, Mode: input.Mode, Addrs: input.Addrs,
		MasterName: input.MasterName, Username: input.Username, Password: password,
		PasswordEnv: passwordEnv, PasswordFile: passwordFile, DB: input.DB,
		TLS: input.TLS, TLSServer: input.TLSServer, TLSCAFile: input.TLSCAFile,
		TLSCertFile: input.TLSCertFile, TLSKeyFile: input.TLSKeyFile, KeyPattern: input.KeyPattern,
	}
	normalized := []connectionConfig{connection}
	if err := validateConnections(normalized); err != nil {
		return connectionConfig{}, err
	}
	return normalized[0], nil
}

func validateRuntimeConfig(config appConfig) error {
	if err := validateConnections(config.Connections); err != nil {
		return err
	}
	manager, err := newRedisManager(config)
	if err != nil {
		return err
	}
	manager.close()
	return nil
}

func publicConnectionConfig(connection connectionConfig) map[string]any {
	return map[string]any{
		"id": connection.ID, "name": connection.Name, "mode": connection.Mode,
		"addrs": connection.Addrs, "masterName": connection.MasterName,
		"username":           connection.Username,
		"passwordConfigured": connection.Password != "" || connection.PasswordEnv != "" || connection.PasswordFile != "",
		"db":                 connection.DB, "tls": connection.TLS, "tlsServerName": connection.TLSServer,
		"tlsCAFile": connection.TLSCAFile, "tlsCertFile": connection.TLSCertFile,
		"tlsKeyFile": connection.TLSKeyFile, "keyPattern": connection.KeyPattern,
	}
}
