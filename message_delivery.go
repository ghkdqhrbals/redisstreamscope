package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	messageDeliveryTimeout      = 5 * time.Second
	messageDeliveryMaxGroups    = 100
	messageDeliveryMaxConsumers = 200
)

var errMessageDeliveryStateChanged = errors.New("consumer group state changed while delivery information was being read")

type messageDeliveryRedis interface {
	XInfoGroups(context.Context, string) *redis.XInfoGroupsCmd
	Pipelined(context.Context, func(redis.Pipeliner) error) ([]redis.Cmder, error)
}

type messageDeliveryGroup struct {
	Group              string   `json:"group"`
	Pending            bool     `json:"pending"`
	Consumer           string   `json:"consumer"`
	IdleMs             int64    `json:"idleMs"`
	DeliveryCount      int64    `json:"deliveryCount"`
	Lag                int64    `json:"lag"`
	LastDeliveredID    string   `json:"lastDeliveredId"`
	AvailableConsumers []string `json:"availableConsumers"`
	ConsumerCount      int      `json:"consumerCount"`
	ConsumersTruncated bool     `json:"consumersTruncated"`
}

type messageDeliveryResponse struct {
	EntryID           string                 `json:"entryId"`
	Groups            []messageDeliveryGroup `json:"groups"`
	TotalGroups       int                    `json:"totalGroups"`
	GroupsTruncated   bool                   `json:"groupsTruncated"`
	NotPendingMeaning string                 `json:"notPendingMeaning"`
}

func buildMessageDelivery(ctx context.Context, client messageDeliveryRedis, streamKey, entryID string) (messageDeliveryResponse, error) {
	response := messageDeliveryResponse{
		EntryID:           entryID,
		Groups:            []messageDeliveryGroup{},
		NotPendingMeaning: "The entry is absent from this group's PEL; this does not prove it was acknowledged.",
	}
	groups, err := client.XInfoGroups(ctx, streamKey).Result()
	if err != nil {
		return response, err
	}
	response.TotalGroups = len(groups)
	sort.Slice(groups, func(left, right int) bool { return groups[left].Name < groups[right].Name })
	if len(groups) > messageDeliveryMaxGroups {
		groups = groups[:messageDeliveryMaxGroups]
		response.GroupsTruncated = true
	}
	if len(groups) == 0 {
		return response, nil
	}

	pendingCommands := make([]*redis.XPendingExtCmd, len(groups))
	pendingCommanders, err := client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for index, group := range groups {
			pendingCommands[index] = pipe.XPendingExt(ctx, &redis.XPendingExtArgs{
				Stream: streamKey,
				Group:  group.Name,
				Start:  entryID,
				End:    entryID,
				Count:  1,
			})
		}
		return nil
	})
	if err != nil {
		return response, messageDeliveryPipelineError(err, groups, pendingCommanders)
	}

	resultGroups := make([]messageDeliveryGroup, len(groups))
	pendingIndexes := make([]int, 0, len(groups))
	for index, group := range groups {
		pendingItems, err := pendingCommands[index].Result()
		if err != nil {
			return response, messageDeliveryGroupReadError(err, group.Name)
		}
		if len(pendingItems) > 1 || (len(pendingItems) == 1 && pendingItems[0].ID != entryID) {
			return response, errors.New("Redis returned an invalid exact XPENDING response")
		}
		item := messageDeliveryGroup{
			Group: group.Name, Lag: group.Lag, LastDeliveredID: group.LastDeliveredID, AvailableConsumers: []string{},
		}
		if len(pendingItems) == 1 {
			pending := pendingItems[0]
			item.Pending = true
			item.Consumer = pending.Consumer
			item.IdleMs = pending.Idle.Milliseconds()
			item.DeliveryCount = pending.RetryCount
			pendingIndexes = append(pendingIndexes, index)
		}
		resultGroups[index] = item
	}

	if len(pendingIndexes) > 0 {
		consumerCommands := make([]*redis.XInfoConsumersCmd, len(pendingIndexes))
		consumerCommanders, err := client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
			for commandIndex, groupIndex := range pendingIndexes {
				consumerCommands[commandIndex] = pipe.XInfoConsumers(ctx, streamKey, groups[groupIndex].Name)
			}
			return nil
		})
		if err != nil {
			pendingGroups := make([]redis.XInfoGroup, len(pendingIndexes))
			for index, groupIndex := range pendingIndexes {
				pendingGroups[index] = groups[groupIndex]
			}
			return response, messageDeliveryPipelineError(err, pendingGroups, consumerCommanders)
		}
		for commandIndex, groupIndex := range pendingIndexes {
			consumers, err := consumerCommands[commandIndex].Result()
			if err != nil {
				return response, messageDeliveryGroupReadError(err, groups[groupIndex].Name)
			}
			consumerNames := make([]string, 0, len(consumers))
			for _, consumer := range consumers {
				consumerNames = append(consumerNames, consumer.Name)
			}
			sort.Strings(consumerNames)
			consumerCount := len(consumerNames)
			consumersTruncated := consumerCount > messageDeliveryMaxConsumers
			if consumersTruncated {
				consumerNames = consumerNames[:messageDeliveryMaxConsumers]
			}
			resultGroups[groupIndex].AvailableConsumers = consumerNames
			resultGroups[groupIndex].ConsumerCount = consumerCount
			resultGroups[groupIndex].ConsumersTruncated = consumersTruncated
		}
	}
	response.Groups = resultGroups
	return response, nil
}

func messageDeliveryPipelineError(pipelineErr error, groups []redis.XInfoGroup, commands []redis.Cmder) error {
	for index, command := range commands {
		if command != nil && isRedisNoGroupError(command.Err()) {
			return fmt.Errorf("%w: %s", errMessageDeliveryStateChanged, groups[index].Name)
		}
	}
	if isRedisNoGroupError(pipelineErr) {
		return errMessageDeliveryStateChanged
	}
	return pipelineErr
}

func messageDeliveryGroupReadError(err error, group string) error {
	if isRedisNoGroupError(err) {
		return fmt.Errorf("%w: %s", errMessageDeliveryStateChanged, group)
	}
	return err
}

func (s *apiServer) messageDelivery(writer http.ResponseWriter, request *http.Request) {
	connectionID := strings.TrimSpace(request.URL.Query().Get("connectionId"))
	streamKey := strings.TrimSpace(request.URL.Query().Get("key"))
	entryID := strings.TrimSpace(request.URL.Query().Get("id"))
	if err := validateRecoveryIdentifier("connectionId", connectionID, 256); err != nil {
		writeError(writer, http.StatusBadRequest, "invalid_connection_id", err.Error())
		return
	}
	if err := validateRecoveryIdentifier("key", streamKey, 1024); err != nil {
		writeError(writer, http.StatusBadRequest, "invalid_stream_key", err.Error())
		return
	}
	if err := validateStreamEntryID("id", entryID, false); err != nil {
		writeError(writer, http.StatusBadRequest, "invalid_entry_id", err.Error())
		return
	}

	connection, err := s.redis.get(connectionID)
	if err != nil {
		writeError(writer, http.StatusBadRequest, "unknown_connection", err.Error())
		return
	}
	checker, err := s.store.permissionChecker(request.Context(), requestSession(request), "groups:read")
	if err != nil {
		writePermissionCheckError(writer)
		return
	}
	if !checker.allows("groups:read", redisStreamScope(connection.config.ID, streamKey)) {
		writeError(writer, http.StatusForbidden, "permission_denied", "this stream is outside the permitted scope")
		return
	}

	ctx, cancel := context.WithTimeout(request.Context(), messageDeliveryTimeout)
	defer cancel()
	response, err := buildMessageDelivery(ctx, connection.client, streamKey, entryID)
	if err != nil {
		switch {
		case errors.Is(err, errMessageDeliveryStateChanged):
			writeError(writer, http.StatusConflict, "delivery_state_changed", errMessageDeliveryStateChanged.Error())
		case isRedisMissingStreamError(err):
			writeError(writer, http.StatusNotFound, "stream_not_found", "stream was not found")
		default:
			writeRedisError(writer, err)
		}
		return
	}
	writeJSON(writer, http.StatusOK, response)
}

func isRedisNoGroupError(err error) bool {
	return err != nil && strings.Contains(strings.ToUpper(err.Error()), "NOGROUP")
}

func isRedisMissingStreamError(err error) bool {
	if errors.Is(err, redis.Nil) {
		return true
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "no such key")
}
