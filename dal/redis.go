package dal

import (
	"context"
	"fmt"
	"strings"

	"github.com/redis/go-redis/v9"
)

type RedisClient interface {
	Increment(ctx context.Context, parts []string) error
	Get(ctx context.Context, parts []string) (int64, error)
}

type internalRedisClient interface {
	Incr(ctx context.Context, key string) *redis.IntCmd
	Get(ctx context.Context, key string) *redis.StringCmd
}
type mockInternalRedisClient struct {
	IncrFn func(ctx context.Context, key string) *redis.IntCmd
	GetFn  func(ctx context.Context, key string) *redis.StringCmd
}

type ProdRedisClient struct {
	client internalRedisClient
}
type MockRedisClient struct {
	IncrementFn func(ctx context.Context, parts []string) error
	GetFn       func(ctx context.Context, parts []string) (int64, error)
}

func (c *mockInternalRedisClient) Incr(ctx context.Context, key string) *redis.IntCmd {
	return c.IncrFn(ctx, key)
}
func (c *mockInternalRedisClient) Get(ctx context.Context, key string) *redis.StringCmd {
	return c.GetFn(ctx, key)
}

func (c *MockRedisClient) Increment(ctx context.Context, parts []string) error {
	return c.IncrementFn(ctx, parts)
}
func (c *MockRedisClient) Get(ctx context.Context, parts []string) (int64, error) {
	return c.GetFn(ctx, parts)
}

func (c *ProdRedisClient) Increment(ctx context.Context, parts []string) error {
	key := strings.Join(parts, " ")
	cmd := c.client.Incr(ctx, key)
	if err := cmd.Err(); err != nil {
		return fmt.Errorf("failed to increment key \"%s\": %w", key, err)
	}
	return nil
}
func (c *ProdRedisClient) Get(ctx context.Context, parts []string) (int64, error) {
	key := strings.Join(parts, " ")
	return c.client.Get(ctx, key).Int64()
}

func NewRedisClient() RedisClient {
	opt := &redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
		Protocol: 2,
	}
	client := redis.NewClient(opt)
	return &ProdRedisClient{client: client}
}
