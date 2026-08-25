package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

type failingPermissionGrantRows struct {
	err error
}

func (rows *failingPermissionGrantRows) Next() bool             { return false }
func (rows *failingPermissionGrantRows) Scan(dest ...any) error { return nil }
func (rows *failingPermissionGrantRows) Err() error             { return rows.err }
func (rows *failingPermissionGrantRows) Close() error           { return nil }

func permissionGrantFailure(kind string) permissionGrantQueryFunc {
	return func(context.Context, string) (permissionGrantRows, error) {
		injected := errors.New("injected permission grant " + kind + " failure")
		if kind == "query" {
			return nil, injected
		}
		return &failingPermissionGrantRows{err: injected}, nil
	}
}

func createScopedACLViewer(t *testing.T, dataStore *store) sessionRecord {
	t.Helper()
	hash, err := hashPassword("viewer-password")
	if err != nil {
		t.Fatal(err)
	}
	user, err := dataStore.createUser(context.Background(), "scope-viewer", "Scope Viewer", hash, "viewer")
	if err != nil {
		t.Fatal(err)
	}
	return sessionRecord{UserID: user.ID, Username: user.Username, Role: user.Role, Enabled: true}
}

func addScopedDeny(t *testing.T, dataStore *store, session sessionRecord, action, streamKey string) {
	t.Helper()
	if _, err := dataStore.upsertGrant(context.Background(), grantRecord{
		UserID: session.UserID, Action: action, Scope: redisStreamScope("redis", streamKey), Effect: "deny",
	}); err != nil {
		t.Fatal(err)
	}
}

func scopedRequest(method, target string, session sessionRecord) *http.Request {
	request := httptest.NewRequest(method, target, nil)
	return request.WithContext(context.WithValue(request.Context(), sessionContextKey, session))
}

func scopedACLServer(dataStore *store) *apiServer {
	return &apiServer{
		store: dataStore,
		redis: &redisManager{
			connections: map[string]*managedRedis{"redis": {config: connectionConfig{ID: "redis", Name: "Redis"}}},
			order:       []string{"redis"},
		},
	}
}

func scopedACLHTTPServer(t *testing.T, dataStore *store) *apiServer {
	t.Helper()
	config := appConfig{SessionTTL: time.Hour, MaxPageSize: 100, MaxLiveStreams: 1}
	server := scopedACLServer(dataStore)
	server.config = config
	server.auth = newAuthenticator(config, dataStore)
	server.tails = newTailBroker(config.MaxLiveStreams)
	server.mux = http.NewServeMux()
	server.started = time.Now()
	server.routes()
	t.Cleanup(func() { operationalMonitors.Delete(server) })
	return server
}

func scopedACLViewerToken(t *testing.T, dataStore *store, viewer sessionRecord) string {
	t.Helper()
	hash, err := hashPassword("viewer-active-password")
	if err != nil {
		t.Fatal(err)
	}
	if err := dataStore.changeOwnPassword(context.Background(), viewer.UserID, hash); err != nil {
		t.Fatal(err)
	}
	user, _, err := dataStore.authenticate(context.Background(), viewer.Username)
	if err != nil {
		t.Fatal(err)
	}
	token, _, err := dataStore.createSession(context.Background(), user, time.Hour, "127.0.0.1", "scoped-acl-test")
	if err != nil {
		t.Fatal(err)
	}
	return token
}

func TestScopedPermissionCheckerPreservesDenyWinsAndAdminSemantics(t *testing.T) {
	dataStore := openMonitoringHistoryTestStore(t)
	viewer := createScopedACLViewer(t, dataStore)
	if viewer.Role != "viewer" {
		t.Fatalf("unexpected test role %q", viewer.Role)
	}
	addScopedDeny(t, dataStore, viewer, "streams:read", "secret")

	checker, err := dataStore.permissionChecker(context.Background(), viewer, "streams:read")
	if err != nil {
		t.Fatal(err)
	}
	if !checker.allows("streams:read", redisStreamScope("redis", "public")) {
		t.Fatal("viewer baseline access was lost")
	}
	if checker.allows("streams:read", redisStreamScope("redis", "secret")) {
		t.Fatal("stream-specific deny was ignored")
	}
	admin, err := dataStore.permissionChecker(context.Background(), sessionRecord{Role: "admin"}, "streams:read")
	if err != nil {
		t.Fatal(err)
	}
	if !admin.allows("streams:read", redisStreamScope("redis", "secret")) {
		t.Fatal("administrator behavior changed")
	}
}

func TestPermissionGrantLoadFailuresFailClosed(t *testing.T) {
	for _, kind := range []string{"query", "rows"} {
		t.Run(kind, func(t *testing.T) {
			dataStore := openMonitoringHistoryTestStore(t)
			viewer := createScopedACLViewer(t, dataStore)
			dataStore.permissionGrantQuery = permissionGrantFailure(kind)

			if _, err := dataStore.permissionChecker(context.Background(), viewer, "streams:read"); err == nil {
				t.Fatal("permission checker accepted an incomplete grant set")
			}
			if dataStore.allowed(context.Background(), viewer, "streams:read", redisStreamScope("redis", "public")) {
				t.Fatal("store.allowed fell back to the viewer role baseline")
			}
			if actions := dataStore.effectivePermissionActions(context.Background(), viewer); len(actions) != 0 {
				t.Fatalf("advisory permissions failed open: %#v", actions)
			}

			adminSession := sessionRecord{Role: "admin"}
			admin, err := dataStore.permissionChecker(context.Background(), adminSession, "streams:read")
			if err != nil || !admin.allows("streams:read", redisStreamScope("redis", "public")) {
				t.Fatalf("administrator bypass changed: checker=%#v err=%v", admin, err)
			}
			if !dataStore.allowed(context.Background(), adminSession, "streams:read", redisStreamScope("redis", "public")) {
				t.Fatal("administrator was denied while the grant store was unavailable")
			}
			if actions := dataStore.effectivePermissionActions(context.Background(), adminSession); len(actions) != 1 || actions[0] != "*" {
				t.Fatalf("administrator advisory permissions changed: %#v", actions)
			}
		})
	}
}

func TestScopedHandlersReturnNoPartialResponseWhenGrantLoadingFails(t *testing.T) {
	dataStore := openMonitoringHistoryTestStore(t)
	viewer := createScopedACLViewer(t, dataStore)
	server := scopedACLServer(dataStore)

	handlers := []struct {
		name    string
		target  string
		handler http.HandlerFunc
	}{
		{name: "operational snapshot", target: "/api/operations/snapshot?connectionId=redis", handler: server.operationsSnapshot},
		{name: "capacity", target: "/api/operations/capacity?connectionId=redis&window=1h", handler: server.capacityForecast},
		{name: "retention policies", target: "/api/operations/retention-policies?connectionId=redis", handler: server.retentionPolicies},
		{name: "latest groups", target: "/api/metrics/consumer-groups/latest?connectionId=redis", handler: server.latestConsumerGroupMetrics},
		{name: "overview", target: "/api/overview?connectionId=redis", handler: server.overview},
		{name: "streams", target: "/api/streams?connectionId=redis", handler: server.streams},
		{name: "consumer history", target: "/api/operations/consumer-history?connectionId=redis&streamKey=public", handler: server.consumerHistory},
		{name: "lifecycle metrics", target: "/api/telemetry/lifecycle/metrics?connectionId=redis&streamKey=public", handler: server.lifecycleMetrics},
		{name: "lifecycle requests", target: "/api/telemetry/lifecycle/requests?connectionId=redis&streamKey=public", handler: server.lifecycleRequests},
		{name: "traces", target: "/api/insights/traces?connectionId=redis&streamKey=public", handler: server.traces},
		{name: "trace detail", target: "/api/telemetry/traces/trace-id", handler: server.traceByID},
	}

	for _, kind := range []string{"query", "rows"} {
		t.Run(kind, func(t *testing.T) {
			dataStore.permissionGrantQuery = permissionGrantFailure(kind)
			for _, test := range handlers {
				t.Run(test.name, func(t *testing.T) {
					response := httptest.NewRecorder()
					test.handler(response, scopedRequest(http.MethodGet, test.target, viewer))
					if response.Code != http.StatusInternalServerError {
						t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
					}
					if strings.Contains(response.Body.String(), "public") || strings.Contains(response.Body.String(), `"items"`) {
						t.Fatalf("partial scoped response leaked: %s", response.Body.String())
					}
					var payload struct {
						Code string `json:"code"`
					}
					if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil || payload.Code != "permission_check_failed" {
						t.Fatalf("unexpected error response: payload=%#v err=%v body=%s", payload, err, response.Body.String())
					}
				})
			}
		})
	}
}

func TestProtectedRouteReturns500WhenGrantLoadingFails(t *testing.T) {
	for _, kind := range []string{"query", "rows"} {
		t.Run(kind, func(t *testing.T) {
			dataStore := openMonitoringHistoryTestStore(t)
			viewer := createScopedACLViewer(t, dataStore)
			token := scopedACLViewerToken(t, dataStore, viewer)
			server := scopedACLHTTPServer(t, dataStore)
			dataStore.permissionGrantQuery = permissionGrantFailure(kind)

			request := httptest.NewRequest(http.MethodGet, "/api/operations/snapshot?connectionId=redis", nil)
			request.AddCookie(&http.Cookie{Name: sessionCookieName, Value: token})
			response := httptest.NewRecorder()
			server.ServeHTTP(response, request)

			if response.Code != http.StatusInternalServerError || !strings.Contains(response.Body.String(), `"code":"permission_check_failed"`) {
				t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
			}
			if strings.Contains(response.Body.String(), `"items"`) || strings.Contains(response.Body.String(), `"streams"`) {
				t.Fatalf("protected route returned partial data: %s", response.Body.String())
			}
		})
	}
}

func TestTraceDetailReturns500WhenHandlerGrantLoadFails(t *testing.T) {
	dataStore := openMonitoringHistoryTestStore(t)
	viewer := createScopedACLViewer(t, dataStore)
	token := scopedACLViewerToken(t, dataStore, viewer)
	server := scopedACLHTTPServer(t, dataStore)
	queryCalls := 0
	dataStore.permissionGrantQuery = func(context.Context, string) (permissionGrantRows, error) {
		queryCalls++
		if queryCalls == 1 {
			return &failingPermissionGrantRows{}, nil
		}
		return nil, errors.New("injected handler permission grant query failure")
	}

	request := httptest.NewRequest(http.MethodGet, "/api/telemetry/traces/cross-stream", nil)
	request.AddCookie(&http.Cookie{Name: sessionCookieName, Value: token})
	response := httptest.NewRecorder()
	server.ServeHTTP(response, request)

	if response.Code != http.StatusInternalServerError || !strings.Contains(response.Body.String(), `"code":"permission_check_failed"`) {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	if queryCalls != 2 {
		t.Fatalf("expected middleware and handler permission loads, got %d", queryCalls)
	}
	if strings.Contains(response.Body.String(), `"spans"`) {
		t.Fatalf("trace detail returned a partial span list: %s", response.Body.String())
	}
}

func TestBroadOperationalCapacityAndPolicyResponsesHideDeniedStreams(t *testing.T) {
	dataStore := openMonitoringHistoryTestStore(t)
	viewer := createScopedACLViewer(t, dataStore)
	addScopedDeny(t, dataStore, viewer, "streams:read", "secret")
	server := scopedACLServer(dataStore)
	t.Cleanup(func() { operationalMonitors.Delete(server) })
	now := time.Now().UTC().Add(-time.Minute)
	memory := int64(256)
	snapshot := operationalSnapshot{
		ConnectionID: "redis", Mode: "standalone", CollectedAt: now, Up: true,
		NodeSampledAt: timePointer(now), StreamSampledAt: timePointer(now), MemorySampledAt: timePointer(now),
		Memory: redisMemoryHealth{UsedBytes: 1024, MaxBytes: 8192},
		Nodes:  []redisNodeHealth{{ID: "node", Address: "redis:6379", Up: true}},
		Streams: []streamOperationalHealth{
			{Key: "public", Available: true, Length: 2, EntriesAdded: 2, MemoryBytes: &memory, Risks: []string{}, Groups: []consumerGroupOperationalHealth{}},
			{Key: "secret", Available: true, Length: 9, EntriesAdded: 9, MemoryBytes: &memory, Risks: []string{"private-risk"}, Groups: []consumerGroupOperationalHealth{}},
		},
		Collector: collectorFreshness{ConnectionID: "redis", MonitoredStreams: 2, SucceededStreams: 2},
	}
	monitor := server.operationalMonitor()
	monitor.mu.Lock()
	monitor.snapshots["redis"] = snapshot
	monitor.mu.Unlock()
	if err := dataStore.recordCapacitySample(context.Background(), snapshot); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"public", "secret"} {
		maxLength := int64(100)
		if _, err := dataStore.upsertRetentionPolicy(context.Background(), retentionPolicy{
			ConnectionID: "redis", StreamKey: key, MaxLength: &maxLength,
		}); err != nil {
			t.Fatal(err)
		}
	}

	operationsResponse := httptest.NewRecorder()
	server.operationsSnapshot(operationsResponse, scopedRequest(http.MethodGet, "/api/operations/snapshot?connectionId=redis", viewer))
	if operationsResponse.Code != http.StatusOK {
		t.Fatalf("operations status=%d body=%s", operationsResponse.Code, operationsResponse.Body.String())
	}
	var operations operationalSnapshot
	if err := json.Unmarshal(operationsResponse.Body.Bytes(), &operations); err != nil {
		t.Fatal(err)
	}
	if len(operations.Streams) != 1 || operations.Streams[0].Key != "public" || operations.Collector.MonitoredStreams != 1 {
		t.Fatalf("denied operational stream leaked: %#v", operations)
	}
	adminOperationsResponse := httptest.NewRecorder()
	server.operationsSnapshot(adminOperationsResponse, scopedRequest(http.MethodGet, "/api/operations/snapshot?connectionId=redis", sessionRecord{Role: "admin"}))
	if adminOperationsResponse.Code != http.StatusOK {
		t.Fatalf("admin operations status=%d body=%s", adminOperationsResponse.Code, adminOperationsResponse.Body.String())
	}
	var adminOperations operationalSnapshot
	if err := json.Unmarshal(adminOperationsResponse.Body.Bytes(), &adminOperations); err != nil {
		t.Fatal(err)
	}
	if len(adminOperations.Streams) != 2 {
		t.Fatalf("administrator broad operational access changed: %#v", adminOperations)
	}

	capacityResponse := httptest.NewRecorder()
	server.capacityForecast(capacityResponse, scopedRequest(http.MethodGet, "/api/operations/capacity?connectionId=redis&window=1h", viewer))
	if capacityResponse.Code != http.StatusOK {
		t.Fatalf("capacity status=%d body=%s", capacityResponse.Code, capacityResponse.Body.String())
	}
	var capacity capacityForecastResponse
	if err := json.Unmarshal(capacityResponse.Body.Bytes(), &capacity); err != nil {
		t.Fatal(err)
	}
	if len(capacity.Streams) != 1 || capacity.Streams[0].StreamKey != "public" {
		t.Fatalf("denied capacity stream leaked: %#v", capacity.Streams)
	}

	policiesResponse := httptest.NewRecorder()
	server.retentionPolicies(policiesResponse, scopedRequest(http.MethodGet, "/api/operations/retention-policies?connectionId=redis", viewer))
	if policiesResponse.Code != http.StatusOK {
		t.Fatalf("policies status=%d body=%s", policiesResponse.Code, policiesResponse.Body.String())
	}
	var policies struct {
		Items []retentionPolicy `json:"items"`
	}
	if err := json.Unmarshal(policiesResponse.Body.Bytes(), &policies); err != nil {
		t.Fatal(err)
	}
	if len(policies.Items) != 1 || policies.Items[0].StreamKey != "public" {
		t.Fatalf("denied retention policy leaked: %#v", policies.Items)
	}
}

func TestConsumerHistoryAndLifecycleRequireAuthorizedExactScopeForNonAdmin(t *testing.T) {
	dataStore := openMonitoringHistoryTestStore(t)
	viewer := createScopedACLViewer(t, dataStore)
	addScopedDeny(t, dataStore, viewer, "groups:read", "secret")
	addScopedDeny(t, dataStore, viewer, "streams:read", "secret")
	server := scopedACLServer(dataStore)
	now := time.Now().UTC().Add(-time.Minute)
	lag := int64(1)
	snapshot := operationalSnapshot{
		ConnectionID: "redis", CollectedAt: now, Up: true, StreamSampledAt: timePointer(now),
		Streams: []streamOperationalHealth{
			{Key: "public", Available: true, Groups: []consumerGroupOperationalHealth{{Name: "workers", Consumers: 1, Lag: &lag, ConsumerSampleStatus: "sampled", ConsumerStates: []consumerOperationalHealth{{Name: "public-worker"}}}}},
			{Key: "secret", Available: true, Groups: []consumerGroupOperationalHealth{{Name: "workers", Consumers: 1, Lag: &lag, ConsumerSampleStatus: "sampled", ConsumerStates: []consumerOperationalHealth{{Name: "secret-worker"}}}}},
		},
	}
	if err := dataStore.recordConsumerHistory(context.Background(), snapshot); err != nil {
		t.Fatal(err)
	}
	var currentGroups int
	if err := dataStore.db.QueryRow(`SELECT COUNT(*) FROM consumer_group_current WHERE connection_id='redis'`).Scan(&currentGroups); err != nil || currentGroups != 2 {
		t.Fatalf("prepare consumer history: groups=%d err=%v", currentGroups, err)
	}
	for _, key := range []string{"public", "secret"} {
		registered := now
		if err := dataStore.ingestLifecycleBatch(context.Background(), []lifecycleEvent{{
			TraceID: key + "-trace", ConnectionID: "redis", StreamKey: key, RegisteredAt: &registered,
		}}); err != nil {
			t.Fatal(err)
		}
	}

	timeQuery := url.Values{
		"connectionId": {"redis"}, "from": {now.Add(-time.Hour).Format(time.RFC3339Nano)},
		"to": {now.Add(time.Hour).Format(time.RFC3339Nano)},
	}
	broadHistory := httptest.NewRecorder()
	broadHistoryRequest := scopedRequest(http.MethodGet, "/api/operations/consumer-history?"+timeQuery.Encode(), viewer)
	if role := requestSession(broadHistoryRequest).Role; role != "viewer" {
		t.Fatalf("session context was lost: %q", role)
	}
	server.consumerHistory(broadHistory, broadHistoryRequest)
	if broadHistory.Code != http.StatusBadRequest {
		t.Fatalf("broad consumer history was allowed: %d %s", broadHistory.Code, broadHistory.Body.String())
	}
	secretHistoryQuery := url.Values{}
	for key, values := range timeQuery {
		secretHistoryQuery[key] = append([]string(nil), values...)
	}
	secretHistoryQuery.Set("streamKey", "secret")
	secretHistory := httptest.NewRecorder()
	server.consumerHistory(secretHistory, scopedRequest(http.MethodGet, "/api/operations/consumer-history?"+secretHistoryQuery.Encode(), viewer))
	if secretHistory.Code != http.StatusForbidden {
		t.Fatalf("denied consumer history was allowed: %d %s", secretHistory.Code, secretHistory.Body.String())
	}
	publicHistoryQuery := url.Values{}
	for key, values := range timeQuery {
		publicHistoryQuery[key] = append([]string(nil), values...)
	}
	publicHistoryQuery.Set("streamKey", "public")
	publicHistory := httptest.NewRecorder()
	server.consumerHistory(publicHistory, scopedRequest(http.MethodGet, "/api/operations/consumer-history?"+publicHistoryQuery.Encode(), viewer))
	if publicHistory.Code != http.StatusOK || !json.Valid(publicHistory.Body.Bytes()) {
		t.Fatalf("authorized consumer history failed: %d %s", publicHistory.Code, publicHistory.Body.String())
	}
	adminHistory := httptest.NewRecorder()
	server.consumerHistory(adminHistory, scopedRequest(http.MethodGet, "/api/operations/consumer-history?"+timeQuery.Encode(), sessionRecord{Role: "admin"}))
	if adminHistory.Code != http.StatusOK || !json.Valid(adminHistory.Body.Bytes()) {
		t.Fatalf("administrator broad consumer history changed: %d %s", adminHistory.Code, adminHistory.Body.String())
	}

	for _, endpoint := range []string{"metrics", "requests"} {
		broad := httptest.NewRecorder()
		serverHandler := server.lifecycleMetrics
		if endpoint == "requests" {
			serverHandler = server.lifecycleRequests
		}
		serverHandler(broad, scopedRequest(http.MethodGet, "/api/telemetry/lifecycle/"+endpoint+"?connectionId=redis", viewer))
		if broad.Code != http.StatusBadRequest {
			t.Fatalf("broad lifecycle %s was allowed: %d %s", endpoint, broad.Code, broad.Body.String())
		}
		denied := httptest.NewRecorder()
		serverHandler(denied, scopedRequest(http.MethodGet, "/api/telemetry/lifecycle/"+endpoint+"?connectionId=redis&streamKey=secret", viewer))
		if denied.Code != http.StatusForbidden {
			t.Fatalf("denied lifecycle %s was allowed: %d %s", endpoint, denied.Code, denied.Body.String())
		}
		allowed := httptest.NewRecorder()
		serverHandler(allowed, scopedRequest(http.MethodGet, "/api/telemetry/lifecycle/"+endpoint+"?connectionId=redis&streamKey=public", viewer))
		if allowed.Code != http.StatusOK || !json.Valid(allowed.Body.Bytes()) {
			t.Fatalf("authorized lifecycle %s failed: %d %s", endpoint, allowed.Code, allowed.Body.String())
		}
		adminBroad := httptest.NewRecorder()
		serverHandler(adminBroad, scopedRequest(http.MethodGet, "/api/telemetry/lifecycle/"+endpoint+"?connectionId=redis", sessionRecord{Role: "admin"}))
		if adminBroad.Code != http.StatusOK || !json.Valid(adminBroad.Body.Bytes()) {
			t.Fatalf("administrator broad lifecycle %s changed: %d %s", endpoint, adminBroad.Code, adminBroad.Body.String())
		}
	}
}

func TestLatestConsumerGroupSnapshotsFilterStreamSpecificDeny(t *testing.T) {
	dataStore := openMonitoringHistoryTestStore(t)
	viewer := createScopedACLViewer(t, dataStore)
	addScopedDeny(t, dataStore, viewer, "groups:read", "secret")
	server := scopedACLServer(dataStore)
	now := time.Now().UTC()
	if err := dataStore.writeConsumerGroupMetricSamples(context.Background(), []consumerGroupMetricSample{
		{RecordedAt: now, ConnectionID: "redis", StreamKey: "public", GroupName: "workers", LagKnown: true},
		{RecordedAt: now, ConnectionID: "redis", StreamKey: "secret", GroupName: "workers", LagKnown: true},
	}); err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	server.latestConsumerGroupMetrics(response, scopedRequest(http.MethodGet, "/api/metrics/consumer-groups/latest?connectionId=redis", viewer))
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	var payload struct {
		Items []consumerGroupMetricSnapshot `json:"items"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if len(payload.Items) != 1 || payload.Items[0].StreamKey != "public" {
		t.Fatalf("denied group snapshot leaked: %#v", payload.Items)
	}
}

func TestAlertReadAPIsFilterDeniedStreamsAndBroadStreamScopes(t *testing.T) {
	dataStore := openMonitoringHistoryTestStore(t)
	viewer := createScopedACLViewer(t, dataStore)
	addScopedDeny(t, dataStore, viewer, "alerts:read", "secret")
	server := scopedACLServer(dataStore)
	ctx := context.Background()
	enabled := true
	createRule := func(name, metric, connectionID, streamKey string) alertRule {
		t.Helper()
		rule, err := dataStore.createAlertRule(ctx, alertRuleInput{
			Name: name, Metric: metric, Operator: ">", Threshold: 1,
			ConnectionID: connectionID, StreamKey: streamKey, Enabled: &enabled,
		}, viewer.UserID)
		if err != nil {
			t.Fatal(err)
		}
		return rule
	}
	publicRule := createRule("public-rule", alertMetricConsumerGroupLag, "redis", "public")
	secretRule := createRule("secret-rule", alertMetricConsumerGroupLag, "redis", "secret")
	_ = createRule("all-streams-rule", alertMetricConsumerGroupLag, "redis", "")
	_ = createRule("connection-health-rule", alertMetricConnectionUp, "redis", "")

	now := time.Now().UTC()
	createIncident := func(id string, rule alertRule, streamKey string) alertIncident {
		t.Helper()
		if _, err := dataStore.db.ExecContext(ctx, `INSERT INTO alert_incidents(
			id,rule_id,status,started_at,updated_at,trigger_value,last_value,summary
		) VALUES(?,?,?,?,?,?,?,?)`, id, rule.ID, alertIncidentFiring,
			formatAlertTime(now), formatAlertTime(now), 2, 2, id+" summary"); err != nil {
			t.Fatal(err)
		}
		if _, err := dataStore.db.ExecContext(ctx, `INSERT INTO alert_incident_labels(
			incident_id,labels_json,connection_id,stream_key,group_name,observed_at,updated_at
		) VALUES(?,?,?,?,?,?,?)`, id, `{}`, "redis", streamKey, "workers", formatAlertTime(now), formatAlertTime(now)); err != nil {
			t.Fatal(err)
		}
		return alertIncident{ID: id, RuleID: rule.ID, ConnectionID: "redis", StreamKey: streamKey}
	}
	publicIncident := createIncident("public-incident", publicRule, "public")
	secretIncident := createIncident("secret-incident", secretRule, "secret")
	for _, fixture := range []struct {
		rule     alertRule
		incident alertIncident
	}{
		{rule: publicRule, incident: publicIncident},
		{rule: secretRule, incident: secretIncident},
	} {
		if err := dataStore.recordAlertWebhookDelivery(ctx, alertNotification{
			Rule: fixture.rule, Incident: fixture.incident, Event: "firing",
		}, 1, http.StatusBadGateway, "failed", fixture.incident.ID+" delivery error", now); err != nil {
			t.Fatal(err)
		}
	}

	rulesResponse := httptest.NewRecorder()
	server.alertRules(rulesResponse, scopedRequest(http.MethodGet, "/api/alert-rules", viewer))
	if rulesResponse.Code != http.StatusOK {
		t.Fatalf("rules status=%d body=%s", rulesResponse.Code, rulesResponse.Body.String())
	}
	if body := rulesResponse.Body.String(); !strings.Contains(body, "public-rule") || !strings.Contains(body, "connection-health-rule") ||
		strings.Contains(body, "secret-rule") || strings.Contains(body, "all-streams-rule") {
		t.Fatalf("alert rule scope filtering failed: %s", body)
	}

	for _, test := range []struct {
		name       string
		rule       alertRule
		wantStatus int
	}{
		{name: "authorized", rule: publicRule, wantStatus: http.StatusOK},
		{name: "denied", rule: secretRule, wantStatus: http.StatusNotFound},
	} {
		t.Run("rule detail "+test.name, func(t *testing.T) {
			request := scopedRequest(http.MethodGet, "/api/alert-rules/"+test.rule.ID, viewer)
			request.SetPathValue("id", test.rule.ID)
			response := httptest.NewRecorder()
			server.alertRuleByID(response, request)
			if response.Code != test.wantStatus {
				t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
			}
		})
	}

	incidentsResponse := httptest.NewRecorder()
	server.alertIncidents(incidentsResponse, scopedRequest(http.MethodGet, "/api/alert-incidents?limit=100", viewer))
	if incidentsResponse.Code != http.StatusOK {
		t.Fatalf("incidents status=%d body=%s", incidentsResponse.Code, incidentsResponse.Body.String())
	}
	if body := incidentsResponse.Body.String(); !strings.Contains(body, publicIncident.ID) || strings.Contains(body, secretIncident.ID) {
		t.Fatalf("alert incident scope filtering failed: %s", body)
	}
	var incidentPayload struct {
		Summary alertDashboardSummary `json:"summary"`
	}
	if err := json.Unmarshal(incidentsResponse.Body.Bytes(), &incidentPayload); err != nil {
		t.Fatal(err)
	}
	if incidentPayload.Summary.Firing != 1 || incidentPayload.Summary.EnabledRules != 2 || incidentPayload.Summary.WebhookFailures != 1 {
		t.Fatalf("scoped alert summary leaked counts: %+v", incidentPayload.Summary)
	}

	deliveriesResponse := httptest.NewRecorder()
	server.alertWebhookDeliveries(deliveriesResponse, scopedRequest(http.MethodGet, "/api/alert-webhook-deliveries?limit=100", viewer))
	if deliveriesResponse.Code != http.StatusOK {
		t.Fatalf("deliveries status=%d body=%s", deliveriesResponse.Code, deliveriesResponse.Body.String())
	}
	if body := deliveriesResponse.Body.String(); !strings.Contains(body, publicIncident.ID) || strings.Contains(body, secretIncident.ID) {
		t.Fatalf("alert delivery scope filtering failed: %s", body)
	}

	adminRules := httptest.NewRecorder()
	server.alertRules(adminRules, scopedRequest(http.MethodGet, "/api/alert-rules", sessionRecord{Role: "admin"}))
	if adminRules.Code != http.StatusOK || !strings.Contains(adminRules.Body.String(), "secret-rule") || !strings.Contains(adminRules.Body.String(), "all-streams-rule") {
		t.Fatalf("administrator alert visibility changed: %d %s", adminRules.Code, adminRules.Body.String())
	}
}

func TestAlertReadHandlersFailClosedWhenGrantLoadingFails(t *testing.T) {
	dataStore := openMonitoringHistoryTestStore(t)
	viewer := createScopedACLViewer(t, dataStore)
	server := scopedACLServer(dataStore)

	for _, kind := range []string{"query", "rows"} {
		t.Run(kind, func(t *testing.T) {
			dataStore.permissionGrantQuery = permissionGrantFailure(kind)
			for _, test := range []struct {
				name    string
				target  string
				handler http.HandlerFunc
			}{
				{name: "rules", target: "/api/alert-rules", handler: server.alertRules},
				{name: "incidents", target: "/api/alert-incidents", handler: server.alertIncidents},
				{name: "deliveries", target: "/api/alert-webhook-deliveries", handler: server.alertWebhookDeliveries},
			} {
				t.Run(test.name, func(t *testing.T) {
					response := httptest.NewRecorder()
					test.handler(response, scopedRequest(http.MethodGet, test.target, viewer))
					if response.Code != http.StatusInternalServerError || !strings.Contains(response.Body.String(), `"code":"permission_check_failed"`) {
						t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
					}
					if strings.Contains(response.Body.String(), `"items"`) {
						t.Fatalf("partial alert data leaked: %s", response.Body.String())
					}
				})
			}
			detailRequest := scopedRequest(http.MethodGet, "/api/alert-rules/hidden", viewer)
			detailRequest.SetPathValue("id", "hidden")
			detailResponse := httptest.NewRecorder()
			server.alertRuleByID(detailResponse, detailRequest)
			if detailResponse.Code != http.StatusInternalServerError || !strings.Contains(detailResponse.Body.String(), `"code":"permission_check_failed"`) {
				t.Fatalf("detail status=%d body=%s", detailResponse.Code, detailResponse.Body.String())
			}
		})
	}
}
