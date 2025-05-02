package cache

import (
	"context"
	"time"

	network "labelwall/biz/model/relationship/network"
)

// NodeCache 定义节点缓存操作接口
type NodeCache interface {
	// GetNode 从缓存中获取节点信息。
	// 如果缓存未命中，应返回 ErrNotFound。
	// 如果缓存了空值（表示DB中不存在），应返回 ErrNilValue。
	GetNode(ctx context.Context, id string) (*network.Node, error)

	// SetNode 将节点信息存入缓存。
	// 实现应处理 TTL 和 TTL Jitter。
	// node 参数可以为 nil，用于缓存空值（防止缓存穿透）。
	SetNode(ctx context.Context, id string, node *network.Node, ttl time.Duration) error

	// DeleteNode 从缓存中删除节点信息。
	// 通常在数据库更新或删除后调用，以保证缓存失效。
	DeleteNode(ctx context.Context, id string) error
}

// RelationCache 定义关系缓存操作接口
type RelationCache interface {
	// GetRelation 从缓存中获取关系信息。
	// 如果缓存未命中，应返回 ErrNotFound。
	// 如果缓存了空值，应返回 ErrNilValue (如果实现了空值缓存)。
	GetRelation(ctx context.Context, id string) (*network.Relation, error)

	// SetRelation 将关系信息存入缓存。
	// 实现应处理 TTL 和 TTL Jitter。
	SetRelation(ctx context.Context, id string, relation *network.Relation, ttl time.Duration) error

	// DeleteRelation 从缓存中删除关系信息。
	DeleteRelation(ctx context.Context, id string) error
}

// Cache 定义了一个通用的缓存操作接口，支持泛型。
// T 代表需要缓存的数据类型。
type Cache[T any] interface {
	// Get 从缓存中获取指定 key 的值。
	// 如果缓存未命中，应返回 ErrNotFound。
	// 如果缓存了空值，应返回 ErrNilValue。
	Get(ctx context.Context, key string) (T, error)

	// Set 将键值对存入缓存，并设置过期时间。
	// 实现应处理 TTL 和 TTL Jitter。
	// value 可以是零值，用于缓存空对象（如果需要）。
	Set(ctx context.Context, key string, value T, ttl time.Duration) error

	// Delete 从缓存中删除指定的 key。
	Delete(ctx context.Context, key string) error
}
