package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/cladder/ke-events/internal/telemetry"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

const EventsChannel = "ke:events:live"

type Client struct {
	rdb *redis.Client
	sf  sync.Map // singleflight-style per-key mutex to prevent cache stampede
}

func New(addr string) *Client {
	rdb := redis.NewClient(&redis.Options{
		Addr:         addr,
		PoolSize:     32,
		MinIdleConns: 4,
		DialTimeout:  3 * time.Second,
	})
	return &Client{rdb: rdb}
}

func (c *Client) Ping(ctx context.Context) error {
	return c.rdb.Ping(ctx).Err()
}

func (c *Client) Close() error {
	return c.rdb.Close()
}

// GetOrLoad implements Cache-Aside with Singleflight + TTL Jitter to prevent Cache Stampede.
func GetOrLoad[T any](ctx context.Context, c *Client, key string, baseTTL time.Duration, loader func(context.Context) (T, error)) (T, error) {
	var zero T
	if raw, err := c.rdb.Get(ctx, key).Bytes(); err == nil {
		var val T
		if json.Unmarshal(raw, &val) == nil {
			telemetry.CacheHitsTotal.Inc()
			return val, nil
		}
	}
	telemetry.CacheMissesTotal.Inc()

	muAny, _ := c.sf.LoadOrStore(key, &sync.Mutex{})
	mu := muAny.(*sync.Mutex)
	mu.Lock()
	defer mu.Unlock()

	// Double-check after acquiring singleflight lock
	if raw, err := c.rdb.Get(ctx, key).Bytes(); err == nil {
		var val T
		if json.Unmarshal(raw, &val) == nil {
			return val, nil
		}
	}

	val, err := loader(ctx)
	if err != nil {
		return zero, err
	}

	if encoded, err := json.Marshal(val); err == nil {
		jitter := time.Duration(rand.Int64N(int64(baseTTL/5) + 1))
		_ = c.rdb.Set(ctx, key, encoded, baseTTL+jitter).Err()
	}
	return val, nil
}

func (c *Client) InvalidatePrefix(ctx context.Context, prefix string) {
	iter := c.rdb.Scan(ctx, 0, prefix+"*", 100).Iterator()
	var keys []string
	for iter.Next(ctx) {
		keys = append(keys, iter.Val())
	}
	if len(keys) > 0 {
		_ = c.rdb.Del(ctx, keys...).Err()
	}
}

// AcquireLock acquires a distributed Redis lock with a unique token.
func (c *Client) AcquireLock(ctx context.Context, key string, ttl time.Duration) (string, bool, error) {
	token := uuid.NewString()
	ok, err := c.rdb.SetNX(ctx, "lock:"+key, token, ttl).Result()
	return token, ok, err
}

var releaseLua = redis.NewScript(`
if redis.call("get", KEYS[1]) == ARGV[1] then
	return redis.call("del", KEYS[1])
else
	return 0
end
`)

func (c *Client) ReleaseLock(ctx context.Context, key, token string) {
	_ = releaseLua.Run(ctx, c.rdb, []string{"lock:" + key}, token).Err()
}

// AllowSlidingWindow enforces a distributed sliding-window rate limit.
func (c *Client) AllowSlidingWindow(ctx context.Context, key string, limit int, window time.Duration) (bool, int64, error) {
	now := time.Now().UnixMilli()
	minScore := fmt.Sprintf("%d", now-window.Milliseconds())
	zkey := "rl:" + key

	pipe := c.rdb.TxPipeline()
	pipe.ZRemRangeByScore(ctx, zkey, "0", minScore)
	countCmd := pipe.ZCard(ctx, zkey)
	pipe.ZAdd(ctx, zkey, redis.Z{Score: float64(now), Member: uuid.NewString()})
	pipe.Expire(ctx, zkey, window)
	if _, err := pipe.Exec(ctx); err != nil {
		return true, 0, err
	}
	count := countCmd.Val()
	return count < int64(limit), count, nil
}

func (c *Client) PublishEvent(ctx context.Context, payload []byte) error {
	return c.rdb.Publish(ctx, EventsChannel, payload).Err()
}

func (c *Client) SubscribeEvents(ctx context.Context) *redis.PubSub {
	return c.rdb.Subscribe(ctx, EventsChannel)
}
