package cache

//TODO: 使用日志库
//TODO: 使用布隆过滤器

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"time"

	network "labelwall/biz/model/relationship/network"

	"github.com/redis/go-redis/v9"
	"github.com/willf/bloom" // Import Bloom filter library
)

const (
	// NilValuePlaceholder 用于在 Redis 中标记空值，以区分 key 不存在和 key 存在但值为空。
	NilValuePlaceholder = "__NIL_VALUE__"
	// NilValueTTL 设置空值的较短 TTL，防止长时间缓存不存在的数据。
	NilValueTTL = 5 * time.Minute
	// DefaultTTLJitterPercent DefaultTTL Jitter 百分比，例如 0.1 表示在基础 TTL 上增加 0% 到 10% 的随机时间。
	DefaultTTLJitterPercent = 0.1
)

// redisCache implements the Cache interface using Redis.
// It includes NodeCache, RelationCache, and generic ByteCache functionality.
type RedisCache struct {
	client *redis.Client
	prefix string             // Prefix for all keys managed by this cache instance
	filter *bloom.BloomFilter // Bloom filter instance
}

// Ensure RedisCache implements all required interfaces.
var _ NodeCache = (*RedisCache)(nil)
var _ RelationCache = (*RedisCache)(nil)
var _ NodeAndByteCache = (*RedisCache)(nil)
var _ RelationAndByteCache = (*RedisCache)(nil)

// NewRedisCache creates a new RedisCache instance.
// estimatedKeys: Estimated number of unique items (nodes + relations + other keys) the cache will hold.
// fpRate: Desired false positive rate for the Bloom filter (e.g., 0.01 for 1%).
func NewRedisCache(client *redis.Client, prefix string, estimatedKeys uint, fpRate float64) (*RedisCache, error) {
	if client == nil {
		return nil, errors.New("redis client cannot be nil")
	}
	// Initialize Bloom filter
	filter := bloom.NewWithEstimates(estimatedKeys, fpRate)

	return &RedisCache{
		client: client,
		prefix: prefix,
		filter: filter,
	}, nil
}

// --- NodeCache Implementation ---

// nodeKey generates the Redis key for a node.
func (c *RedisCache) nodeKey(id string) string {
	return c.prefix + "node:" + id
}

// GetNode retrieves a node from the cache.
func (c *RedisCache) GetNode(ctx context.Context, id string) (*network.Node, error) {
	key := c.nodeKey(id)

	// 1. Check Bloom Filter first
	if !c.filter.TestString(key) {
		// If the filter says the key definitely doesn't exist, return NotFound
		return nil, ErrNotFound
	}

	// 2. Proceed to check Redis if filter test passes (key might exist)
	valBytes, err := c.client.Get(ctx, key).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, ErrNotFound // Key not found in Redis
		}
		return nil, fmt.Errorf("redis Get failed for node %s: %w", id, err)
	}

	// Check for nil placeholder
	if bytes.Equal(valBytes, []byte(NilValuePlaceholder)) {
		return nil, ErrNilValue
	}

	var node network.Node
	if err := json.Unmarshal(valBytes, &node); err != nil {
		return nil, fmt.Errorf("failed to unmarshal node %s: %w", id, err)
	}
	return &node, nil
}

// SetNode stores a node in the cache.
// If node is nil, it stores a placeholder indicating absence.
func (c *RedisCache) SetNode(ctx context.Context, id string, node *network.Node, ttl time.Duration) error {
	key := c.nodeKey(id)
	var valBytes []byte
	var err error

	if node == nil {
		valBytes = []byte(NilValuePlaceholder)
		// DO NOT add nil placeholders to the Bloom filter
	} else {
		valBytes, err = json.Marshal(node)
		if err != nil {
			return fmt.Errorf("failed to marshal node %s: %w", id, err)
		}
		// Add the key to the Bloom filter *only* if storing a real node
	}

	// Add the key to the Bloom filter regardless of whether it's a placeholder or real node.
	// This allows GetNode to find nil placeholders after passing the filter check.
	c.filter.AddString(key)

	// Apply jitter to TTL before setting
	ttlWithJitter := addJitter(ttl)

	if err := c.client.Set(ctx, key, valBytes, ttlWithJitter).Err(); err != nil {
		// Consider if we should attempt to remove from filter if Set fails? Might be overly complex.
		return fmt.Errorf("redis Set failed for node %s: %w", id, err)
	}
	return nil
}

// DeleteNode removes a node from the cache.
func (c *RedisCache) DeleteNode(ctx context.Context, id string) error {
	key := c.nodeKey(id)
	// Note: Standard Bloom filters don't support deletion easily.
	// We simply delete from Redis. If the item is re-added later, the filter
	// might already contain it (which is acceptable). False negatives are avoided.
	if err := c.client.Del(ctx, key).Err(); err != nil {
		if errors.Is(err, redis.Nil) {
			return ErrNotFound // Or return nil, as deleting non-existent is often okay
		}
		return fmt.Errorf("redis Del failed for node %s: %w", id, err)
	}
	return nil
}

// --- RelationCache Implementation ---

// relationKey generates the Redis key for a relation.
func (c *RedisCache) relationKey(id string) string {
	return c.prefix + "relation:" + id
}

// GetRelation retrieves a relation from the cache.
func (c *RedisCache) GetRelation(ctx context.Context, id string) (*network.Relation, error) {
	key := c.relationKey(id)

	// 1. Check Bloom Filter first
	if !c.filter.TestString(key) {
		return nil, ErrNotFound
	}

	// 2. Check Redis
	valBytes, err := c.client.Get(ctx, key).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("redis Get failed for relation %s: %w", id, err)
	}

	if bytes.Equal(valBytes, []byte(NilValuePlaceholder)) {
		return nil, ErrNilValue
	}

	var relation network.Relation
	if err := json.Unmarshal(valBytes, &relation); err != nil {
		return nil, fmt.Errorf("failed to unmarshal relation %s: %w", id, err)
	}
	return &relation, nil
}

// SetRelation stores a relation in the cache.
func (c *RedisCache) SetRelation(ctx context.Context, id string, relation *network.Relation, ttl time.Duration) error {
	key := c.relationKey(id)
	var valBytes []byte
	var err error

	if relation == nil {
		valBytes = []byte(NilValuePlaceholder)
		// DO NOT add nil placeholders to the Bloom filter
	} else {
		valBytes, err = json.Marshal(relation)
		if err != nil {
			return fmt.Errorf("failed to marshal relation %s: %w", id, err)
		}
		// Add the key to the Bloom filter *only* if storing a real relation
	}

	// Add the key to the Bloom filter regardless of whether it's a placeholder or real relation.
	c.filter.AddString(key)

	// Apply jitter to TTL before setting
	ttlWithJitter := addJitter(ttl)

	if err := c.client.Set(ctx, key, valBytes, ttlWithJitter).Err(); err != nil {
		return fmt.Errorf("redis Set failed for relation %s: %w", id, err)
	}
	return nil
}

// DeleteRelation removes a relation from the cache.
func (c *RedisCache) DeleteRelation(ctx context.Context, id string) error {
	key := c.relationKey(id)
	// No deletion from standard Bloom filter.
	if err := c.client.Del(ctx, key).Err(); err != nil {
		if errors.Is(err, redis.Nil) {
			return ErrNotFound // Or return nil
		}
		return fmt.Errorf("redis Del failed for relation %s: %w", id, err)
	}
	return nil
}

// --- ByteCache Implementation ---

// Get retrieves generic byte data from the cache.
func (c *RedisCache) Get(ctx context.Context, key string) ([]byte, error) {
	// Assume generic keys might not be in the main node/relation Bloom filter,
	// OR use a separate filter, OR add them if appropriate.
	// For simplicity, let's bypass the Bloom filter check for generic Get/Set for now.
	// If these keys represent entities that *should* be filtered, add the check:
	/*
		if !c.filter.TestString(c.prefix + key) { // Use prefixed key
			return nil, ErrNotFound
		}
	*/

	fullKey := c.prefix + key
	valBytes, err := c.client.Get(ctx, fullKey).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("redis Get failed for key %s: %w", key, err)
	}

	if bytes.Equal(valBytes, []byte(NilValuePlaceholder)) {
		return nil, ErrNilValue
	}
	return valBytes, nil
}

// Set stores generic byte data in the cache.
func (c *RedisCache) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	fullKey := c.prefix + key
	// If value is nil, use the placeholder, otherwise use the value.
	valToStore := value
	// addKeyToFilter := true // <<< Removed variable
	if value == nil {
		valToStore = []byte(NilValuePlaceholder)
		// addKeyToFilter = false // Don't add nil placeholders to filter // <<< Logic removed
	}

	// Add to Bloom filter regardless of nil placeholder.
	// Decide if *all* generic keys should be added or only specific ones based on usage patterns.
	// For now, adding all keys prefixed by this cache instance.
	c.filter.AddString(fullKey)

	// Apply jitter to TTL before setting
	ttlWithJitter := addJitter(ttl)

	if err := c.client.Set(ctx, fullKey, valToStore, ttlWithJitter).Err(); err != nil {
		return fmt.Errorf("redis Set failed for key %s: %w", key, err)
	}

	// Add to Bloom filter only if it wasn't a nil placeholder // <<< Logic removed and moved up
	// Consider if *all* generic keys should be added or only specific ones.
	// if addKeyToFilter {
	//     c.filter.AddString(fullKey) // Decide if generic keys go into the filter
	// }
	return nil
}

// Delete removes generic byte data from the cache.
func (c *RedisCache) Delete(ctx context.Context, key string) error {
	fullKey := c.prefix + key
	if err := c.client.Del(ctx, fullKey).Err(); err != nil {
		if errors.Is(err, redis.Nil) {
			return ErrNotFound // Or return nil
		}
		return fmt.Errorf("redis Del failed for key %s: %w", key, err)
	}
	return nil
}

// --- 辅助函数 ---

// addJitter 为 TTL 增加随机偏移，防止缓存雪崩
func addJitter(baseTTL time.Duration) time.Duration {
	if baseTTL <= 0 {
		return baseTTL // 0 或负数 TTL 通常表示不过期或立即过期，不添加 jitter
	}
	jitter := time.Duration(rand.Float64() * DefaultTTLJitterPercent * float64(baseTTL))
	return baseTTL + jitter
}
