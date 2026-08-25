package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestBuildMessageDeliveryReportsExactPELStateAcrossGroups(t *testing.T) {
	client := &fakeMessageDeliveryClient{
		groups: []redis.XInfoGroup{
			{Name: "workers-b", Lag: -1, LastDeliveredID: "10-0"},
			{Name: "workers-a", Lag: 7, LastDeliveredID: "9-0"},
		},
		pending: map[string][]redis.XPendingExt{
			"workers-a": {{ID: "8-0", Consumer: "worker-2", Idle: 3 * time.Second, RetryCount: 4}},
			"workers-b": {},
		},
		consumers: map[string][]redis.XInfoConsumer{
			"workers-a": {{Name: "worker-2"}, {Name: "worker-1"}},
			"workers-b": {{Name: "worker-3"}},
		},
	}

	response, err := buildMessageDelivery(context.Background(), client, "orders", "8-0")
	if err != nil {
		t.Fatal(err)
	}
	if response.EntryID != "8-0" || response.TotalGroups != 2 || response.GroupsTruncated || len(response.Groups) != 2 {
		t.Fatalf("unexpected response summary: %#v", response)
	}
	if !strings.Contains(response.NotPendingMeaning, "does not prove it was acknowledged") {
		t.Fatalf("PEL absence was represented as a proven acknowledgement: %q", response.NotPendingMeaning)
	}
	first := response.Groups[0]
	if first.Group != "workers-a" || !first.Pending || first.Consumer != "worker-2" || first.IdleMs != 3000 || first.DeliveryCount != 4 || first.Lag != 7 || first.LastDeliveredID != "9-0" {
		t.Fatalf("pending delivery state was not preserved: %#v", first)
	}
	if first.ConsumerCount != 2 || first.ConsumersTruncated || strings.Join(first.AvailableConsumers, ",") != "worker-1,worker-2" {
		t.Fatalf("consumer names were not stable and complete: %#v", first)
	}
	second := response.Groups[1]
	if second.Group != "workers-b" || second.Pending || second.Consumer != "" || second.IdleMs != 0 || second.DeliveryCount != 0 || second.Lag != -1 {
		t.Fatalf("non-pending delivery state was overstated: %#v", second)
	}
	if len(client.pendingArgs) != 2 {
		t.Fatalf("expected one exact XPENDING query per group, got %d", len(client.pendingArgs))
	}
	for _, arguments := range client.pendingArgs {
		if arguments.Stream != "orders" || arguments.Start != "8-0" || arguments.End != "8-0" || arguments.Count != 1 {
			t.Fatalf("XPENDING query was not exact and bounded: %#v", arguments)
		}
	}
	if client.consumerQueries != 1 || strings.Join(client.consumerGroups, ",") != "workers-a" {
		t.Fatalf("consumers were queried for a non-pending group: queries=%d groups=%v", client.consumerQueries, client.consumerGroups)
	}
	if client.pipelineCalls != 2 || len(client.pipelineCommandCounts) != 2 || client.pipelineCommandCounts[0] != 2 || client.pipelineCommandCounts[1] != 1 {
		t.Fatalf("delivery lookup did not use two bounded query phases: calls=%d commands=%v", client.pipelineCalls, client.pipelineCommandCounts)
	}
}

func TestBuildMessageDeliveryBoundsGroupsAndConsumers(t *testing.T) {
	groups := make([]redis.XInfoGroup, messageDeliveryMaxGroups+1)
	pending := make(map[string][]redis.XPendingExt, len(groups))
	consumers := make(map[string][]redis.XInfoConsumer, len(groups))
	for index := range groups {
		name := fmt.Sprintf("group-%03d", index)
		groups[index] = redis.XInfoGroup{Name: name}
		pending[name] = []redis.XPendingExt{}
		if index == 0 {
			pending[name] = []redis.XPendingExt{{ID: "8-0", Consumer: "consumer-000"}}
			consumers[name] = make([]redis.XInfoConsumer, messageDeliveryMaxConsumers+1)
			for consumerIndex := range consumers[name] {
				consumers[name][consumerIndex].Name = fmt.Sprintf("consumer-%03d", consumerIndex)
			}
		}
	}
	client := &fakeMessageDeliveryClient{groups: groups, pending: pending, consumers: consumers}
	response, err := buildMessageDelivery(context.Background(), client, "orders", "8-0")
	if err != nil {
		t.Fatal(err)
	}
	if response.TotalGroups != messageDeliveryMaxGroups+1 || !response.GroupsTruncated || len(response.Groups) != messageDeliveryMaxGroups {
		t.Fatalf("group work was not bounded: total=%d truncated=%v returned=%d", response.TotalGroups, response.GroupsTruncated, len(response.Groups))
	}
	if len(client.pendingArgs) != messageDeliveryMaxGroups || client.consumerQueries != 1 {
		t.Fatalf("per-group Redis work exceeded its bound: pending=%d consumers=%d", len(client.pendingArgs), client.consumerQueries)
	}
	if client.pipelineCalls != 2 || len(client.pipelineCommandCounts) != 2 || client.pipelineCommandCounts[0] != messageDeliveryMaxGroups || client.pipelineCommandCounts[1] != 1 {
		t.Fatalf("pipeline phases exceeded bounds: calls=%d commands=%v", client.pipelineCalls, client.pipelineCommandCounts)
	}
	first := response.Groups[0]
	if first.ConsumerCount != messageDeliveryMaxConsumers+1 || !first.ConsumersTruncated || len(first.AvailableConsumers) != messageDeliveryMaxConsumers {
		t.Fatalf("consumer response was not bounded: %#v", first)
	}
}

func TestBuildMessageDeliveryRejectsGroupRaceWithoutPartialResponse(t *testing.T) {
	client := &fakeMessageDeliveryClient{
		groups:     []redis.XInfoGroup{{Name: "workers"}},
		pendingErr: map[string]error{"workers": errors.New("NOGROUP No such key or consumer group")},
	}
	response, err := buildMessageDelivery(context.Background(), client, "orders", "8-0")
	if !errors.Is(err, errMessageDeliveryStateChanged) {
		t.Fatalf("expected group-race error, response=%#v err=%v", response, err)
	}
	if len(response.Groups) != 0 {
		t.Fatalf("partial group state was returned after a race: %#v", response.Groups)
	}
}

func TestMessageDeliveryEnforcesExactStreamScopeAndFailsClosed(t *testing.T) {
	t.Run("exact deny", func(t *testing.T) {
		dataStore := openMonitoringHistoryTestStore(t)
		viewer := createScopedACLViewer(t, dataStore)
		addScopedDeny(t, dataStore, viewer, "groups:read", "secret")
		client := &fakeMessageDeliveryClient{groups: []redis.XInfoGroup{{Name: "workers"}}}
		server := messageDeliveryTestServer(dataStore, client)

		denied := httptest.NewRecorder()
		server.messageDelivery(denied, scopedRequest(http.MethodGet, "/api/message-delivery?connectionId=redis&key=secret&id=8-0", viewer))
		if denied.Code != http.StatusForbidden || !strings.Contains(denied.Body.String(), `"code":"permission_denied"`) {
			t.Fatalf("exact stream deny was ignored: %d %s", denied.Code, denied.Body.String())
		}
		if client.groupQueries != 0 {
			t.Fatalf("Redis was queried before exact ACL enforcement: %d", client.groupQueries)
		}

		allowed := httptest.NewRecorder()
		server.messageDelivery(allowed, scopedRequest(http.MethodGet, "/api/message-delivery?connectionId=redis&key=public&id=8-0", viewer))
		if allowed.Code != http.StatusOK {
			t.Fatalf("authorized stream failed: %d %s", allowed.Code, allowed.Body.String())
		}
		var response messageDeliveryResponse
		if err := json.Unmarshal(allowed.Body.Bytes(), &response); err != nil || response.EntryID != "8-0" || len(response.Groups) != 1 {
			t.Fatalf("unexpected authorized response: %#v err=%v", response, err)
		}
		if !strings.Contains(allowed.Body.String(), `"group":"workers"`) || strings.Contains(allowed.Body.String(), `"name":"workers"`) {
			t.Fatalf("message-delivery JSON contract regressed: %s", allowed.Body.String())
		}
	})

	t.Run("grant read failure", func(t *testing.T) {
		dataStore := openMonitoringHistoryTestStore(t)
		viewer := createScopedACLViewer(t, dataStore)
		dataStore.permissionGrantQuery = permissionGrantFailure("query")
		client := &fakeMessageDeliveryClient{groups: []redis.XInfoGroup{{Name: "workers"}}}
		server := messageDeliveryTestServer(dataStore, client)
		response := httptest.NewRecorder()
		server.messageDelivery(response, scopedRequest(http.MethodGet, "/api/message-delivery?connectionId=redis&key=public&id=8-0", viewer))
		if response.Code != http.StatusInternalServerError || !strings.Contains(response.Body.String(), `"code":"permission_check_failed"`) {
			t.Fatalf("permission store error did not fail closed: %d %s", response.Code, response.Body.String())
		}
		if client.groupQueries != 0 {
			t.Fatalf("Redis was queried after a permission failure: %d", client.groupQueries)
		}
	})
}

func TestMessageDeliveryValidatesExactAddressingAndRedisErrors(t *testing.T) {
	dataStore := openMonitoringHistoryTestStore(t)
	server := messageDeliveryTestServer(dataStore, &fakeMessageDeliveryClient{})
	admin := sessionRecord{Role: "admin"}
	for _, target := range []string{
		"/api/message-delivery?key=orders&id=1-0",
		"/api/message-delivery?connectionId=redis&id=1-0",
		"/api/message-delivery?connectionId=redis&key=orders&id=invalid",
	} {
		response := httptest.NewRecorder()
		server.messageDelivery(response, scopedRequest(http.MethodGet, target, admin))
		if response.Code != http.StatusBadRequest {
			t.Fatalf("invalid request %q returned %d: %s", target, response.Code, response.Body.String())
		}
	}
	if !isRedisMissingStreamError(redis.Nil) || !isRedisMissingStreamError(errors.New("ERR no such key")) || isRedisMissingStreamError(errors.New("connection reset")) {
		t.Fatal("missing-stream Redis errors were classified incorrectly")
	}
}

func messageDeliveryTestServer(dataStore *store, client *fakeMessageDeliveryClient) *apiServer {
	return &apiServer{
		store: dataStore,
		redis: &redisManager{
			connections: map[string]*managedRedis{
				"redis": {config: connectionConfig{ID: "redis", Name: "Redis"}, client: &fakeMessageDeliveryUniversalClient{fake: client}},
			},
			order: []string{"redis"},
		},
	}
}

type fakeMessageDeliveryUniversalClient struct {
	redis.UniversalClient
	fake *fakeMessageDeliveryClient
}

func (client *fakeMessageDeliveryUniversalClient) XInfoGroups(ctx context.Context, stream string) *redis.XInfoGroupsCmd {
	return client.fake.XInfoGroups(ctx, stream)
}

func (client *fakeMessageDeliveryUniversalClient) XPendingExt(ctx context.Context, arguments *redis.XPendingExtArgs) *redis.XPendingExtCmd {
	return client.fake.XPendingExt(ctx, arguments)
}

func (client *fakeMessageDeliveryUniversalClient) XInfoConsumers(ctx context.Context, stream, group string) *redis.XInfoConsumersCmd {
	return client.fake.XInfoConsumers(ctx, stream, group)
}

func (client *fakeMessageDeliveryUniversalClient) Pipelined(ctx context.Context, callback func(redis.Pipeliner) error) ([]redis.Cmder, error) {
	return client.fake.Pipelined(ctx, callback)
}

type fakeMessageDeliveryClient struct {
	groups                []redis.XInfoGroup
	groupsErr             error
	pending               map[string][]redis.XPendingExt
	pendingErr            map[string]error
	consumers             map[string][]redis.XInfoConsumer
	consumerErr           map[string]error
	groupQueries          int
	pendingArgs           []*redis.XPendingExtArgs
	consumerQueries       int
	consumerGroups        []string
	pipelineCalls         int
	pipelineCommandCounts []int
}

func (client *fakeMessageDeliveryClient) XInfoGroups(ctx context.Context, stream string) *redis.XInfoGroupsCmd {
	client.groupQueries++
	command := redis.NewXInfoGroupsCmd(ctx, stream)
	if client.groupsErr != nil {
		command.SetErr(client.groupsErr)
	} else {
		command.SetVal(append([]redis.XInfoGroup(nil), client.groups...))
	}
	return command
}

func (client *fakeMessageDeliveryClient) XPendingExt(ctx context.Context, arguments *redis.XPendingExtArgs) *redis.XPendingExtCmd {
	copyArguments := *arguments
	client.pendingArgs = append(client.pendingArgs, &copyArguments)
	command := redis.NewXPendingExtCmd(ctx)
	if err := client.pendingErr[arguments.Group]; err != nil {
		command.SetErr(err)
	} else {
		command.SetVal(append([]redis.XPendingExt(nil), client.pending[arguments.Group]...))
	}
	return command
}

func (client *fakeMessageDeliveryClient) XInfoConsumers(ctx context.Context, stream, group string) *redis.XInfoConsumersCmd {
	client.consumerQueries++
	client.consumerGroups = append(client.consumerGroups, group)
	command := redis.NewXInfoConsumersCmd(ctx, stream, group)
	if err := client.consumerErr[group]; err != nil {
		command.SetErr(err)
	} else {
		command.SetVal(append([]redis.XInfoConsumer(nil), client.consumers[group]...))
	}
	return command
}

func (client *fakeMessageDeliveryClient) Pipelined(ctx context.Context, callback func(redis.Pipeliner) error) ([]redis.Cmder, error) {
	client.pipelineCalls++
	pipeline := &fakeMessageDeliveryPipeline{client: client, commands: []redis.Cmder{}}
	if err := callback(pipeline); err != nil {
		return pipeline.commands, err
	}
	client.pipelineCommandCounts = append(client.pipelineCommandCounts, len(pipeline.commands))
	for _, command := range pipeline.commands {
		if err := command.Err(); err != nil {
			return pipeline.commands, err
		}
	}
	return pipeline.commands, nil
}

type fakeMessageDeliveryPipeline struct {
	redis.Pipeliner
	client   *fakeMessageDeliveryClient
	commands []redis.Cmder
}

func (pipeline *fakeMessageDeliveryPipeline) XPendingExt(ctx context.Context, arguments *redis.XPendingExtArgs) *redis.XPendingExtCmd {
	command := pipeline.client.XPendingExt(ctx, arguments)
	pipeline.commands = append(pipeline.commands, command)
	return command
}

func (pipeline *fakeMessageDeliveryPipeline) XInfoConsumers(ctx context.Context, stream, group string) *redis.XInfoConsumersCmd {
	command := pipeline.client.XInfoConsumers(ctx, stream, group)
	pipeline.commands = append(pipeline.commands, command)
	return command
}
