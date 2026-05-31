package service

import (
	"context"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
)

// moveDueRetriesScript atomically moves all events whose scheduled time has
// passed from the retry_schedule sorted set into the retry_queue list.
// This prevents race conditions when multiple scheduler instances run.
const moveDueRetriesScript = `
local items = redis.call('ZRANGEBYSCORE', KEYS[1], '0', ARGV[1])
if #items > 0 then
	redis.call('ZREM', KEYS[1], unpack(items))
	for _, v in ipairs(items) do
		redis.call('LPUSH', KEYS[2], v)
    end
end
return #items
`

type RedisClient struct {
	client *redis.Client
}

var retryScheduleKey = os.Getenv("RETRY_SCHEDULE")
var retryQueueKey = os.Getenv("RETRY_QUEUE")
var dlqKey = os.Getenv("DLQ_KEY")
var processedEventTTL = 48 * time.Hour

func NewRedisClient(addr string) *RedisClient {
	rdb := redis.NewClient(&redis.Options{
		Addr: addr,
	})
	return &RedisClient{client: rdb}
}

func (r *RedisClient) TryLock(ctx context.Context, eventID string) (bool, error) {
	key := "event_lock:" + eventID
	set, err := r.client.SetArgs(ctx, key, "processing", redis.SetArgs{
		TTL:  time.Minute * 5,
		Mode: "NX",
	}).Result()
	if err != nil {
		return false, err
	}
	return set == "OK", nil
}

// func (r *RedisClient) IsProcessed(ctx context.Context, eventID string) (bool, error) {
// 	exists, err := r.client.Exists(ctx, eventID).Result()
// 	return exists == 1, err
// }

func (r *RedisClient) MarkProcessed(ctx context.Context, eventID string) error {
	return r.client.Set(ctx, eventID, "1", processedEventTTL).Err()
}

// ScheduleRetry places an event into the retry_schedule sorted set with a
// score equal to the Unix millisecond timestamp at which it should next be
// attempted. The scheduler goroutine moves it to retry_queue when ready.
func (r *RedisClient) ScheduleRetry(ctx context.Context, data []byte, delay time.Duration) error {
	score := float64(time.Now().Add(delay).UnixMilli())
	return r.client.ZAdd(ctx, retryScheduleKey, redis.Z{
		Score:  score,
		Member: string(data),
	}).Err()
}

// MoveDueRetries runs the Lua script that atomically moves all events whose
// scheduled time has passed into retry_queue. Returns the count moved.
func (r *RedisClient) MoveDueRetries(ctx context.Context) (int64, error) {
	nowMs := time.Now().UnixMilli()
	result, err := r.client.Eval(
		ctx,
		moveDueRetriesScript,
		[]string{retryScheduleKey, retryQueueKey},
		nowMs,
	).Int64()
	return result, err
}

// PushToDLQ appends a failed event to the dead-letter queue.
func (r *RedisClient) PushToDLQ(ctx context.Context, data []byte) error {
	return r.client.LPush(ctx, dlqKey, data).Err()
}

// PopRetryMessage blocks until a message is available in retry_queue or the
// timeout elapses. Returns (nil, nil) on timeout so callers can loop cleanly.
// Key names are fully contained here — no other package needs to know them.
func (r *RedisClient) PopRetryMessage(ctx context.Context, timeout time.Duration) ([]byte, error) {
	results, err := r.client.BRPop(ctx, timeout, retryQueueKey).Result()
	if err != nil {
		if err == redis.Nil {
			// Timeout — not an error, just nothing ready yet.
			return nil, nil
		}
		return nil, err
	}
	// BRPop returns [key, value].
	return []byte(results[1]), nil
}

func (r *RedisClient) Client() *redis.Client {
	return r.client
}
