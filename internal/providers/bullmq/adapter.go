package bullmq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/gama/queuescope/internal/domain"
	"github.com/gama/queuescope/internal/providers"
	"github.com/redis/go-redis/v9"
)

const (
	defaultRedisURL = "redis://localhost:6379"
	defaultPrefix   = "bull"
)

type Adapter struct{}

func New() *Adapter {
	return &Adapter{}
}

func (a *Adapter) Info() domain.ProviderInfo {
	return domain.ProviderInfo{
		ID:   domain.ProviderBullMQ,
		Name: "BullMQ / Redis",
		Capabilities: []string{
			providers.CapabilityListQueues,
			providers.CapabilityGetQueueStats,
			providers.CapabilityListMessages,
			providers.CapabilityInspectMessage,
			providers.CapabilityRetryMessage,
			providers.CapabilityDeleteMessage,
		},
	}
}

func (a *Adapter) Test(ctx context.Context, connection domain.QueueConnection) error {
	client, err := redisClient(connection)
	if err != nil {
		return err
	}
	defer client.Close()

	return client.Ping(ctx).Err()
}

func (a *Adapter) ListQueues(ctx context.Context, connection domain.QueueConnection) ([]domain.QueueInfo, error) {
	client, err := redisClient(connection)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	prefix := connectionPrefix(connection)
	keys, err := scanKeys(ctx, client, prefix+":*:meta")
	if err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		keys, err = discoverQueueKeysFromKnownStateKeys(ctx, client, prefix)
		if err != nil {
			return nil, err
		}
	}

	queues := make([]domain.QueueInfo, 0, len(keys))
	for _, key := range keys {
		queueName := strings.TrimSuffix(strings.TrimPrefix(key, prefix+":"), ":meta")
		stats, err := queueStats(ctx, client, prefix, queueName)
		if err != nil {
			return nil, err
		}

		queues = append(queues, domain.QueueInfo{
			ID:           queueName,
			Name:         queueName,
			Provider:     domain.ProviderBullMQ,
			ConnectionID: connection.ID,
			Stats:        stats,
		})
	}

	return queues, nil
}

func (a *Adapter) ListMessages(ctx context.Context, connection domain.QueueConnection, queueName string, filter providers.MessageFilter) ([]domain.QueueMessage, error) {
	client, err := redisClient(connection)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	prefix := connectionPrefix(connection)
	statuses := bullMQStatuses(filter.Status)
	limit := normalizeLimit(filter.Limit)

	messages := make([]domain.QueueMessage, 0, limit)
	for _, status := range statuses {
		if len(messages) >= limit {
			break
		}

		ids, err := jobIDsByStatus(ctx, client, queueKey(prefix, queueName, statusKeySuffix(status)), status, limit-len(messages))
		if err != nil {
			return nil, err
		}

		for _, id := range ids {
			message, err := readJob(ctx, client, prefix, queueName, id, status)
			if err != nil {
				if errors.Is(err, redis.Nil) {
					continue
				}
				return nil, err
			}
			if filter.Query != "" && !matchesQuery(message, filter.Query) {
				continue
			}
			messages = append(messages, message)
		}
	}

	return messages, nil
}

func (a *Adapter) GetMessage(ctx context.Context, connection domain.QueueConnection, queueName string, messageID string) (domain.QueueMessage, error) {
	client, err := redisClient(connection)
	if err != nil {
		return domain.QueueMessage{}, err
	}
	defer client.Close()

	prefix := connectionPrefix(connection)
	statuses := bullMQStatuses("")
	for _, status := range statuses {
		message, err := readJob(ctx, client, prefix, queueName, messageID, status)
		if err == nil {
			return message, nil
		}
		if !errors.Is(err, redis.Nil) {
			return domain.QueueMessage{}, err
		}
	}

	return domain.QueueMessage{}, errors.New("message not found")
}

func (a *Adapter) RetryMessage(ctx context.Context, connection domain.QueueConnection, queueName string, messageID string) error {
	client, err := redisClient(connection)
	if err != nil {
		return err
	}
	defer client.Close()

	prefix := connectionPrefix(connection)
	jobKey := jobKey(prefix, queueName, messageID)
	exists, err := client.Exists(ctx, jobKey).Result()
	if err != nil {
		return err
	}
	if exists == 0 {
		return errors.New("message not found")
	}

	removed, err := client.ZRem(ctx, queueKey(prefix, queueName, "failed"), messageID).Result()
	if err != nil {
		return err
	}
	if removed == 0 {
		return errors.New("message is not in failed state")
	}

	targetKey := queueKey(prefix, queueName, "wait")
	paused, err := client.HGet(ctx, queueKey(prefix, queueName, "meta"), "paused").Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return err
	}
	if paused != "" {
		targetKey = queueKey(prefix, queueName, "paused")
	}

	pipe := client.TxPipeline()
	pipe.HDel(ctx, jobKey, "finishedOn", "processedOn", "failedReason")
	pipe.LPush(ctx, targetKey, messageID)
	pipe.ZAdd(ctx, queueKey(prefix, queueName, "marker"), redis.Z{Score: 0, Member: "0"})
	pipe.XAdd(ctx, &redis.XAddArgs{
		Stream: queueKey(prefix, queueName, "events"),
		MaxLen: 10000,
		Approx: true,
		Values: map[string]any{
			"event": "waiting",
			"jobId": messageID,
			"prev":  "failed",
		},
	})
	_, err = pipe.Exec(ctx)
	return err
}

func (a *Adapter) DeleteMessage(ctx context.Context, connection domain.QueueConnection, queueName string, messageID string) error {
	client, err := redisClient(connection)
	if err != nil {
		return err
	}
	defer client.Close()

	prefix := connectionPrefix(connection)
	key := jobKey(prefix, queueName, messageID)
	exists, err := client.Exists(ctx, key).Result()
	if err != nil {
		return err
	}
	if exists == 0 {
		return errors.New("message not found")
	}

	pipe := client.TxPipeline()
	pipe.LRem(ctx, queueKey(prefix, queueName, "wait"), 0, messageID)
	pipe.LRem(ctx, queueKey(prefix, queueName, "active"), 0, messageID)
	pipe.LRem(ctx, queueKey(prefix, queueName, "paused"), 0, messageID)
	pipe.ZRem(ctx, queueKey(prefix, queueName, "delayed"), messageID)
	pipe.ZRem(ctx, queueKey(prefix, queueName, "failed"), messageID)
	pipe.ZRem(ctx, queueKey(prefix, queueName, "completed"), messageID)
	pipe.ZRem(ctx, queueKey(prefix, queueName, "prioritized"), messageID)
	pipe.Del(ctx, key, key+":logs", key+":dependencies", key+":processed", key+":failed", key+":unsuccessful")
	pipe.XAdd(ctx, &redis.XAddArgs{
		Stream: queueKey(prefix, queueName, "events"),
		MaxLen: 10000,
		Approx: true,
		Values: map[string]any{
			"event": "removed",
			"jobId": messageID,
		},
	})
	_, err = pipe.Exec(ctx)
	return err
}

func redisClient(connection domain.QueueConnection) (*redis.Client, error) {
	redisURL := stringConfig(connection, "redisUrl", defaultRedisURL)
	options, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, err
	}
	return redis.NewClient(options), nil
}

func connectionPrefix(connection domain.QueueConnection) string {
	return stringConfig(connection, "prefix", defaultPrefix)
}

func stringConfig(connection domain.QueueConnection, key string, fallback string) string {
	value, ok := connection.Config[key].(string)
	if !ok || value == "" {
		return fallback
	}
	return value
}

func scanKeys(ctx context.Context, client *redis.Client, pattern string) ([]string, error) {
	var cursor uint64
	keys := []string{}

	for {
		batch, nextCursor, err := client.Scan(ctx, cursor, pattern, 100).Result()
		if err != nil {
			return nil, err
		}
		keys = append(keys, batch...)
		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}

	return keys, nil
}

func discoverQueueKeysFromKnownStateKeys(ctx context.Context, client *redis.Client, prefix string) ([]string, error) {
	stateKeys, err := scanKeys(ctx, client, prefix+":*")
	if err != nil {
		return nil, err
	}

	queueNames := map[string]struct{}{}
	for _, key := range stateKeys {
		parts := strings.Split(key, ":")
		if len(parts) < 3 || parts[0] != prefix {
			continue
		}
		if isKnownQueueSuffix(parts[len(parts)-1]) {
			queueNames[parts[1]] = struct{}{}
		}
	}

	keys := make([]string, 0, len(queueNames))
	for queueName := range queueNames {
		keys = append(keys, queueKey(prefix, queueName, "meta"))
	}
	return keys, nil
}

func isKnownQueueSuffix(suffix string) bool {
	switch suffix {
	case "wait", "active", "paused", "delayed", "failed", "completed", "prioritized", "events", "marker":
		return true
	default:
		return false
	}
}

func queueStats(ctx context.Context, client *redis.Client, prefix string, queueName string) (domain.QueueStats, error) {
	waiting, err := listLen(ctx, client, queueKey(prefix, queueName, "wait"))
	if err != nil {
		return domain.QueueStats{}, err
	}
	active, err := listLen(ctx, client, queueKey(prefix, queueName, "active"))
	if err != nil {
		return domain.QueueStats{}, err
	}
	delayed, err := zsetLen(ctx, client, queueKey(prefix, queueName, "delayed"))
	if err != nil {
		return domain.QueueStats{}, err
	}
	completed, err := zsetLen(ctx, client, queueKey(prefix, queueName, "completed"))
	if err != nil {
		return domain.QueueStats{}, err
	}
	failed, err := zsetLen(ctx, client, queueKey(prefix, queueName, "failed"))
	if err != nil {
		return domain.QueueStats{}, err
	}

	return domain.QueueStats{
		Waiting:   &waiting,
		Active:    &active,
		Delayed:   &delayed,
		Completed: &completed,
		Failed:    &failed,
	}, nil
}

func listLen(ctx context.Context, client *redis.Client, key string) (int, error) {
	count, err := client.LLen(ctx, key).Result()
	return int(count), err
}

func zsetLen(ctx context.Context, client *redis.Client, key string) (int, error) {
	count, err := client.ZCard(ctx, key).Result()
	return int(count), err
}

func queueKey(prefix string, queueName string, suffix string) string {
	return fmt.Sprintf("%s:%s:%s", prefix, queueName, suffix)
}

func jobKey(prefix string, queueName string, jobID string) string {
	return fmt.Sprintf("%s:%s:%s", prefix, queueName, jobID)
}

func bullMQStatuses(status domain.MessageStatus) []domain.MessageStatus {
	if status != "" && status != domain.StatusUnknown {
		return []domain.MessageStatus{status}
	}
	return []domain.MessageStatus{
		domain.StatusWaiting,
		domain.StatusActive,
		domain.StatusDelayed,
		domain.StatusFailed,
		domain.StatusCompleted,
	}
}

func jobIDsByStatus(ctx context.Context, client *redis.Client, key string, status domain.MessageStatus, limit int) ([]string, error) {
	stop := int64(limit - 1)
	switch status {
	case domain.StatusWaiting, domain.StatusActive:
		return client.LRange(ctx, key, 0, stop).Result()
	case domain.StatusDelayed, domain.StatusFailed, domain.StatusCompleted:
		return client.ZRevRange(ctx, key, 0, stop).Result()
	default:
		return []string{}, nil
	}
}

func statusKeySuffix(status domain.MessageStatus) string {
	if status == domain.StatusWaiting {
		return "wait"
	}
	return string(status)
}

func readJob(ctx context.Context, client *redis.Client, prefix string, queueName string, id string, status domain.MessageStatus) (domain.QueueMessage, error) {
	values, err := client.HGetAll(ctx, jobKey(prefix, queueName, id)).Result()
	if err != nil {
		return domain.QueueMessage{}, err
	}
	if len(values) == 0 {
		return domain.QueueMessage{}, redis.Nil
	}

	payload := decodeJSON(values["data"])
	opts := decodeJSON(values["opts"])
	attempts := parseOptionalInt(values["attemptsMade"])
	createdAt := parseUnixMillis(values["timestamp"])
	startedAt := parseUnixMillis(values["processedOn"])
	completedAt := parseUnixMillis(values["finishedOn"])
	var failedAt *time.Time
	if status == domain.StatusFailed {
		failedAt = completedAt
	}

	metadata := map[string]any{
		"name":        values["name"],
		"opts":        opts,
		"stacktrace":  decodeJSON(values["stacktrace"]),
		"returnvalue": decodeJSON(values["returnvalue"]),
	}

	return domain.QueueMessage{
		ID:          id,
		QueueName:   queueName,
		Provider:    domain.ProviderBullMQ,
		Status:      status,
		Payload:     payload,
		Metadata:    metadata,
		Attempts:    attempts,
		CreatedAt:   createdAt,
		StartedAt:   startedAt,
		CompletedAt: completedAt,
		FailedAt:    failedAt,
		Error:       values["failedReason"],
	}, nil
}

func parseOptionalInt(raw string) *int {
	if raw == "" {
		return nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return nil
	}
	return &value
}

func parseUnixMillis(raw string) *time.Time {
	if raw == "" {
		return nil
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || value <= 0 {
		return nil
	}
	parsed := time.UnixMilli(value).UTC()
	return &parsed
}

func decodeJSON(raw string) any {
	if raw == "" {
		return nil
	}

	var value any
	if err := json.Unmarshal([]byte(raw), &value); err != nil {
		return raw
	}
	return value
}

func normalizeLimit(limit int) int {
	if limit <= 0 {
		return 50
	}
	if limit > 200 {
		return 200
	}
	return limit
}

func matchesQuery(message domain.QueueMessage, query string) bool {
	query = strings.ToLower(strings.TrimSpace(query))
	if query == "" {
		return true
	}

	encoded, _ := json.Marshal(message)
	return strings.Contains(strings.ToLower(string(encoded)), query)
}
