package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

type deadlineRecorder struct {
	*httptest.ResponseRecorder
	deadline time.Time
}

func (writer *deadlineRecorder) SetWriteDeadline(deadline time.Time) error {
	writer.deadline = deadline
	return nil
}

func TestConnectionInputAcceptsPasswordConfiguredState(t *testing.T) {
	request := httptest.NewRequest("PUT", "/api/settings", strings.NewReader(`{
		"id":"redis",
		"name":"Redis",
		"mode":"standalone",
		"addrs":["host.docker.internal:6379"],
		"passwordConfigured":true
	}`))
	var input connectionInput
	if err := readJSON(request, &input, 8<<10); err != nil {
		t.Fatalf("passwordConfigured must be accepted: %v", err)
	}
	if !input.PasswordSet {
		t.Fatal("passwordConfigured state was not decoded")
	}
	if input.Password != "" {
		t.Fatal("passwordConfigured must not become a Redis password")
	}
}

func TestConnectionInputAllowsEmptyPassword(t *testing.T) {
	input := connectionInput{
		ID: "redis", Name: "Redis", Mode: "standalone",
		Addrs: []string{"host.docker.internal:6379"},
	}
	connection, err := mergeConnectionInput(input, connectionConfig{})
	if err != nil {
		t.Fatalf("password-free Redis connection must be accepted: %v", err)
	}
	if connection.Password != "" {
		t.Fatal("empty password must remain empty")
	}
}

func TestEmptyPasswordKeepsExistingSecretDuringEdit(t *testing.T) {
	input := connectionInput{
		ID: "redis", Name: "Redis", Mode: "standalone",
		Addrs: []string{"host.docker.internal:6379"}, PasswordSet: true,
	}
	existing := connectionConfig{
		ID: "redis", Name: "Redis", Mode: "standalone",
		Addrs: []string{"host.docker.internal:6379"}, Password: "saved-secret",
	}
	connection, err := mergeConnectionInput(input, existing)
	if err != nil {
		t.Fatal(err)
	}
	if connection.Password != "saved-secret" {
		t.Fatal("editing with a blank password must retain the stored secret")
	}
}

func TestConsumersRequiresGroup(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/api/consumers?connectionId=redis&key=orders", nil)
	response := httptest.NewRecorder()

	(&apiServer{}).consumers(response, request)

	if response.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, response.Code)
	}
	if !strings.Contains(response.Body.String(), "group_required") {
		t.Fatalf("expected group_required error, got %s", response.Body.String())
	}
}

func TestXAddArgsSupportMaxLenModes(t *testing.T) {
	approximate := newXAddArgs("orders", "*", map[string]string{"id": "1"}, 1000, false)
	if approximate.MaxLen != 1000 || !approximate.Approx {
		t.Fatalf("expected approximate MAXLEN 1000, got maxLen=%d approx=%v", approximate.MaxLen, approximate.Approx)
	}

	exact := newXAddArgs("orders", "*", map[string]string{"id": "1"}, 1000, true)
	if exact.MaxLen != 1000 || exact.Approx {
		t.Fatalf("expected exact MAXLEN 1000, got maxLen=%d approx=%v", exact.MaxLen, exact.Approx)
	}

	unbounded := newXAddArgs("orders", "*", map[string]string{"id": "1"}, 0, false)
	if unbounded.MaxLen != 0 || unbounded.Approx {
		t.Fatalf("expected MAXLEN to be omitted, got maxLen=%d approx=%v", unbounded.MaxLen, unbounded.Approx)
	}
}

func TestStatusWriterExposesStreamingControls(t *testing.T) {
	underlying := &deadlineRecorder{ResponseRecorder: httptest.NewRecorder(), deadline: time.Now()}
	writer := &statusWriter{ResponseWriter: underlying, status: http.StatusOK}

	if err := http.NewResponseController(writer).SetWriteDeadline(time.Time{}); err != nil {
		t.Fatalf("expected wrapped response writer to expose write deadlines: %v", err)
	}
	if !underlying.deadline.IsZero() {
		t.Fatal("expected the SSE write deadline to be disabled")
	}
}

func TestTailStartIDPrefersEventSourceResumeHeader(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/api/tail?lastId=$", nil)
	request.Header.Set("Last-Event-ID", "1722235000000-7")

	if actual := tailStartID(request); actual != "1722235000000-7" {
		t.Fatalf("expected resume ID from Last-Event-ID, got %q", actual)
	}
}

func TestTailSubscriptionRemovesStoppedWorkerSynchronously(t *testing.T) {
	client := redis.NewClient(&redis.Options{
		Addr:        "127.0.0.1:0",
		DialTimeout: time.Millisecond,
	})
	defer client.Close()
	connection := &managedRedis{
		config: connectionConfig{ID: "test"},
		client: client,
	}
	broker := newTailBroker(1)

	subscription, err := broker.subscribe(connection, "orders", "$")
	if err != nil {
		t.Fatal(err)
	}
	subscription.Cancel()

	broker.mu.Lock()
	workers := len(broker.workers)
	broker.mu.Unlock()
	if workers != 0 {
		t.Fatalf("expected the stopped worker to be removed before a reconnect, got %d workers", workers)
	}
}
