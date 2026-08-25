package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestMetricsRouteAuthenticationFromEnvironment(t *testing.T) {
	t.Run("token unset disables the endpoint", func(t *testing.T) {
		server := newMetricsRouteTestServer(t, "")
		request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		if response.Code != http.StatusNotFound {
			t.Fatalf("expected status %d, got %d: %s", http.StatusNotFound, response.Code, response.Body.String())
		}
		if strings.Contains(response.Body.String(), "redisstreamscope_") {
			t.Fatalf("disabled metrics endpoint exposed Prometheus data: %s", response.Body.String())
		}

		direct := httptest.NewRecorder()
		server.prometheusMetrics(direct, request)
		if direct.Code != http.StatusNotFound {
			t.Fatalf("handler must fail closed without a token: status=%d body=%s", direct.Code, direct.Body.String())
		}
	})

	t.Run("configured token rejects missing and invalid bearer credentials", func(t *testing.T) {
		server := newMetricsRouteTestServer(t, "metrics-secret")
		for name, authorization := range map[string]string{
			"missing":      "",
			"wrong bearer": "Bearer not-the-secret",
			"wrong scheme": "Basic metrics-secret",
		} {
			t.Run(name, func(t *testing.T) {
				request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
				if authorization != "" {
					request.Header.Set("Authorization", authorization)
				}
				response := httptest.NewRecorder()

				server.ServeHTTP(response, request)

				if response.Code != http.StatusUnauthorized {
					t.Fatalf("expected status %d, got %d: %s", http.StatusUnauthorized, response.Code, response.Body.String())
				}
				if response.Header().Get("WWW-Authenticate") != `Bearer realm="metrics"` {
					t.Fatalf("expected Bearer challenge, got %q", response.Header().Get("WWW-Authenticate"))
				}
				if !strings.Contains(response.Body.String(), `"code":"metrics_authentication_required"`) {
					t.Fatalf("unexpected authentication error: %s", response.Body.String())
				}
			})
		}
	})

	t.Run("configured token accepts the matching bearer credential", func(t *testing.T) {
		server := newMetricsRouteTestServer(t, "metrics-secret")
		request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
		request.Header.Set("Authorization", "Bearer metrics-secret")
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertPrometheusResponse(t, response)
	})
}

func TestOperationsSnapshotRouteRequiresAuthenticationAndStreamsReadPermission(t *testing.T) {
	config := appConfig{
		DataPath:       filepath.Join(t.TempDir(), "redisstreamscope.db"),
		SessionTTL:     time.Hour,
		MaxPageSize:    100,
		MaxLiveStreams: 1,
	}
	dataStore, err := openStore(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dataStore.close() })
	manager := &redisManager{connections: make(map[string]*managedRedis)}
	server := &apiServer{
		config:  config,
		store:   dataStore,
		redis:   manager,
		auth:    newAuthenticator(config, dataStore),
		tails:   newTailBroker(config.MaxLiveStreams),
		mux:     http.NewServeMux(),
		started: time.Now(),
	}
	server.routes()
	t.Cleanup(func() { operationalMonitors.Delete(server) })

	unauthenticated := httptest.NewRecorder()
	server.ServeHTTP(
		unauthenticated,
		httptest.NewRequest(http.MethodGet, "/api/operations/snapshot?connectionId=redis", nil),
	)
	if unauthenticated.Code != http.StatusUnauthorized {
		t.Fatalf("expected unauthenticated status %d, got %d: %s", http.StatusUnauthorized, unauthenticated.Code, unauthenticated.Body.String())
	}
	if !strings.Contains(unauthenticated.Body.String(), `"code":"authentication_required"`) {
		t.Fatalf("unexpected unauthenticated response: %s", unauthenticated.Body.String())
	}
	if unauthenticated.Header().Get("X-Request-ID") == "" {
		t.Fatal("protected route must assign a request ID")
	}

	ctx := context.Background()
	adminHash, err := hashPassword("admin-password")
	if err != nil {
		t.Fatal(err)
	}
	admin, err := dataStore.createInitialAdmin(ctx, "admin", "Administrator", adminHash)
	if err != nil {
		t.Fatal(err)
	}
	viewerHash, err := hashPassword("viewer-password")
	if err != nil {
		t.Fatal(err)
	}
	viewer, err := dataStore.createUser(ctx, "viewer", "Viewer", viewerHash, "viewer")
	if err != nil {
		t.Fatal(err)
	}
	if err := dataStore.changeOwnPassword(ctx, viewer.ID, viewerHash); err != nil {
		t.Fatal(err)
	}
	viewer, _, err = dataStore.authenticate(ctx, viewer.Username)
	if err != nil || viewer.PasswordChangeRequired {
		t.Fatalf("prepare viewer session: user=%#v err=%v", viewer, err)
	}
	if _, err := dataStore.upsertGrant(ctx, grantRecord{
		UserID: viewer.ID,
		Action: "streams:read",
		Scope:  "connection:redis",
		Effect: "deny",
	}); err != nil {
		t.Fatal(err)
	}
	viewerToken, _, err := dataStore.createSession(ctx, viewer, time.Hour, "127.0.0.1", "route-test")
	if err != nil {
		t.Fatal(err)
	}

	deniedRequest := httptest.NewRequest(http.MethodGet, "/api/operations/snapshot?connectionId=redis", nil)
	deniedRequest.AddCookie(&http.Cookie{Name: sessionCookieName, Value: viewerToken})
	denied := httptest.NewRecorder()
	server.ServeHTTP(denied, deniedRequest)
	if denied.Code != http.StatusForbidden {
		t.Fatalf("expected denied status %d, got %d: %s", http.StatusForbidden, denied.Code, denied.Body.String())
	}
	if !strings.Contains(denied.Body.String(), `"code":"permission_denied"`) {
		t.Fatalf("unexpected permission response: %s", denied.Body.String())
	}

	adminToken, _, err := dataStore.createSession(ctx, admin, time.Hour, "127.0.0.1", "route-test")
	if err != nil {
		t.Fatal(err)
	}
	allowedRequest := httptest.NewRequest(http.MethodGet, "/api/operations/snapshot?connectionId=redis", nil)
	allowedRequest.AddCookie(&http.Cookie{Name: sessionCookieName, Value: adminToken})
	allowed := httptest.NewRecorder()
	server.ServeHTTP(allowed, allowedRequest)
	if allowed.Code != http.StatusBadRequest || !strings.Contains(allowed.Body.String(), `"code":"unknown_connection"`) {
		t.Fatalf("administrator should pass protection and reach the handler, got %d: %s", allowed.Code, allowed.Body.String())
	}

	waitForRouteAccessLogs(t, dataStore, 2)
}

func newMetricsRouteTestServer(t *testing.T, metricsToken string) *apiServer {
	t.Helper()
	t.Setenv("PORT", "18080")
	t.Setenv("CONFIG_PATH", filepath.Join(t.TempDir(), "config.properties"))
	t.Setenv("DATA_PATH", filepath.Join(t.TempDir(), "redisstreamscope.db"))
	t.Setenv("METRICS_TOKEN", metricsToken)
	// Prevent the caller's Redis environment from changing this isolated config.
	// An explicitly empty Redis variable still means "configured" to loadConfig,
	// so the variables must be absent rather than set to an empty string.
	for _, name := range []string{"REDIS_HOST", "REDIS_NODES", "REDIS_URL"} {
		value, present := os.LookupEnv(name)
		if err := os.Unsetenv(name); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if present {
				_ = os.Setenv(name, value)
			} else {
				_ = os.Unsetenv(name)
			}
		})
	}
	config, err := loadConfig()
	if err != nil {
		t.Fatal(err)
	}
	if config.MetricsToken != metricsToken {
		t.Fatalf("METRICS_TOKEN was not loaded: got %q", config.MetricsToken)
	}
	server := &apiServer{config: config, mux: http.NewServeMux()}
	server.routes()
	t.Cleanup(func() { operationalMonitors.Delete(server) })
	return server
}

func assertPrometheusResponse(t *testing.T, response *httptest.ResponseRecorder) {
	t.Helper()
	if response.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d: %s", http.StatusOK, response.Code, response.Body.String())
	}
	if contentType := response.Header().Get("Content-Type"); contentType != "text/plain; version=0.0.4; charset=utf-8" {
		t.Fatalf("unexpected Prometheus content type %q", contentType)
	}
	if !strings.HasPrefix(response.Body.String(), "# HELP redisstreamscope_collector_fresh ") {
		t.Fatalf("expected Prometheus text exposition, got: %s", response.Body.String())
	}
}

func waitForRouteAccessLogs(t *testing.T, dataStore *store, expected int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		items, err := dataStore.listAccessLogs(context.Background(), expected+1)
		if err != nil {
			t.Fatal(err)
		}
		if len(items) >= expected {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %d access logs; found %d", expected, len(items))
		}
		time.Sleep(5 * time.Millisecond)
	}
}
