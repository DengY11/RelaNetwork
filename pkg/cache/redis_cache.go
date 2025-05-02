package cache

//TODO: 使用日志库

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"time"

	network "labelwall/biz/model/relationship/network"

	"github.com/redis/go-redis/v9"
)

const (
	// NilValuePlaceholder 用于在 Redis 中标记空值，以区分 key 不存在和 key 存在但值为空。
	NilValuePlaceholder = "__NIL_VALUE__"
	// NilValueTTL 设置空值的较短 TTL，防止长时间缓存不存在的数据。
	NilValueTTL = 5 * time.Minute
	// DefaultTTL Jitter 百分比，例如 0.1 表示在基础 TTL 上增加 0% 到 10% 的随机时间。
	DefaultTTLJitterPercent = 0.1
)

// redisCache 实现了 NodeCache 和 RelationCache 接口
type redisCache struct {
	client *redis.Client
	prefix string // 添加 prefix 字段，用于区分 key
}

// NewRedisCache 创建一个新的 Redis 缓存实例
func NewRedisCache(client *redis.Client, prefix string) (*redisCache, error) { // 添加 prefix 参数和 error 返回值
	if client == nil {
		return nil, errors.New("cache: redis client cannot be nil")
	}
	return &redisCache{
		client: client,
		prefix: prefix, // 赋值 prefix
	}, nil
}

// --- NodeCache 实现 ---

func (r *redisCache) nodeKey(id string) string {
	// 完成 TODO: 考虑添加 prefix
	return fmt.Sprintf("%snode:%s", r.prefix, id) // 使用 prefix
}

// GetNode 实现 NodeCache 的 GetNode 方法
func (r *redisCache) GetNode(ctx context.Context, id string) (*network.Node, error) {
	key := r.nodeKey(id)
	val, err := r.client.Get(ctx, key).Result()

	if errors.Is(err, redis.Nil) {
		// Key 不存在
		return nil, ErrNotFound
	} else if err != nil {
		// 其他 Redis 错误
		return nil, fmt.Errorf("cache: redis get failed for key %s: %w", key, err)
	}

	if val == NilValuePlaceholder {
		// 缓存了空值
		return nil, ErrNilValue
	}

	// 反序列化
	var node network.Node
	if err := json.Unmarshal([]byte(val), &node); err != nil {
		// 数据损坏或类型不匹配，可以考虑删除这个 key
		r.client.Del(ctx, key) // 尝试删除损坏的数据
		return nil, fmt.Errorf("cache: failed to unmarshal node data for key %s: %w", key, err)
	}

	return &node, nil
}

// SetNode 实现 NodeCache 的 SetNode 方法
func (r *redisCache) SetNode(ctx context.Context, id string, node *network.Node, ttl time.Duration) error {
	key := r.nodeKey(id)

	if node == nil {
		// 缓存空值
		// 注意：空值的 TTL 通常较短，这里直接使用 NilValueTTL
		err := r.client.Set(ctx, key, NilValuePlaceholder, addJitter(NilValueTTL)).Err()
		if err != nil {
			return fmt.Errorf("cache: redis set nil value failed for key %s: %w", key, err)
		}
		return nil
	}

	// 序列化节点数据
	data, err := json.Marshal(node)
	if err != nil {
		return fmt.Errorf("cache: failed to marshal node data for key %s: %w", key, err)
	}

	// 设置带 Jitter 的 TTL
	finalTTL := addJitter(ttl)
	err = r.client.Set(ctx, key, data, finalTTL).Err()
	if err != nil {
		return fmt.Errorf("cache: redis set failed for key %s: %w", key, err)
	}

	return nil
}

// DeleteNode 实现 NodeCache 的 DeleteNode 方法
func (r *redisCache) DeleteNode(ctx context.Context, id string) error {
	key := r.nodeKey(id)
	err := r.client.Del(ctx, key).Err()
	// 如果 key 不存在，Del 操作也会成功返回，所以通常不需要特殊处理 redis.Nil
	if err != nil {
		return fmt.Errorf("cache: redis del failed for key %s: %w", key, err)
	}
	return nil
}

// --- RelationCache 实现 (与 NodeCache 非常相似) ---

func (r *redisCache) relationKey(id string) string {
	// 完成 TODO: 考虑添加 prefix
	return fmt.Sprintf("%srelation:%s", r.prefix, id) // 使用 prefix
}

// GetRelation 实现 RelationCache 的 GetRelation 方法
func (r *redisCache) GetRelation(ctx context.Context, id string) (*network.Relation, error) {
	key := r.relationKey(id)
	val, err := r.client.Get(ctx, key).Result()

	if errors.Is(err, redis.Nil) {
		return nil, ErrNotFound
	} else if err != nil {
		return nil, fmt.Errorf("cache: redis get failed for key %s: %w", key, err)
	}

	if val == NilValuePlaceholder {
		// 关系通常不会缓存空值，因为它们的存在依赖于节点。根据业务决定。
		// 如果确定要缓存关系的空值，返回 ErrNilValue
		return nil, ErrNotFound // 默认为未找到
	}

	var relation network.Relation
	if err := json.Unmarshal([]byte(val), &relation); err != nil {
		r.client.Del(ctx, key) // 尝试删除损坏的数据
		return nil, fmt.Errorf("cache: failed to unmarshal relation data for key %s: %w", key, err)
	}

	return &relation, nil
}

// SetRelation 实现 RelationCache 的 SetRelation 方法
func (r *redisCache) SetRelation(ctx context.Context, id string, relation *network.Relation, ttl time.Duration) error {
	key := r.relationKey(id)

	if relation == nil {
		// 通常不缓存空关系，如果需要，取消下面注释并实现
		// err := r.client.Set(ctx, key, NilValuePlaceholder, addJitter(NilValueTTL)).Err()
		// if err != nil {
		//  return fmt.Errorf("cache: redis set nil relation failed for key %s: %w", key, err)
		// }
		return nil // 默认不做任何事
	}

	data, err := json.Marshal(relation)
	if err != nil {
		return fmt.Errorf("cache: failed to marshal relation data for key %s: %w", key, err)
	}

	finalTTL := addJitter(ttl)
	err = r.client.Set(ctx, key, data, finalTTL).Err()
	if err != nil {
		return fmt.Errorf("cache: redis set failed for key %s: %w", key, err)
	}

	return nil
}

// DeleteRelation 实现 RelationCache 的 DeleteRelation 方法
func (r *redisCache) DeleteRelation(ctx context.Context, id string) error {
	key := r.relationKey(id)
	err := r.client.Del(ctx, key).Err()
	if err != nil {
		return fmt.Errorf("cache: redis del failed for key %s: %w", key, err)
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

// 确保 redisCache 同时实现了 NodeCache 和 RelationCache 接口
var _ NodeCache = (*redisCache)(nil)
var _ RelationCache = (*redisCache)(nil)
