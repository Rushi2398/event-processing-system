package service

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisClient struct {
	client *redis.Client
}

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
	return r.client.Set(ctx, eventID, "1", 0).Err()
}

func (r *RedisClient) Client() *redis.Client {
	return r.client
}
