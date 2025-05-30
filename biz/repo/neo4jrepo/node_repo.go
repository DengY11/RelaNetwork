package neo4jrepo

import (
	"bytes"
	"context" // 用于错误处理
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors" // 用于缓存错误检查
	"fmt"    // 用于错误检查
	"sort"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/google/uuid"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/dbtype"

	"labelwall/biz/dal/neo4jdal"
	network "labelwall/biz/model/relationship/network"
	"labelwall/pkg/cache" // 引入缓存包
)

const (
	// 默认节点缓存时间
	// 大部分常量已改为从config中读取，但为了防止潜在的依赖问题，仍然保留
	defaultNodeTTL = 1 * time.Hour

	// SearchNodesCachePrefix is the prefix for search nodes cache keys
	SearchNodesCachePrefix = "search:nodes:ids:"
	// SearchNodesCacheTTL is the TTL for search results cache
	SearchNodesCacheTTL = 5 * time.Minute // 搜索结果缓存 5 分钟
	// SearchEmptyPlaceholder marks empty search results
	SearchEmptyPlaceholder = "__EMPTY_SEARCH__"
	// SearchEmptyTTL is the TTL for empty search results
	SearchEmptyTTL = 1 * time.Minute

	// GetNetworkCachePrefix is the prefix for get network cache keys
	GetNetworkCachePrefix = "network:graph:ids:"
	// GetNetworkCacheTTL is the TTL for network graph cache
	GetNetworkCacheTTL = 10 * time.Minute // 网络图谱缓存 10 分钟 (比搜索长一些)
	// GetNetworkEmptyPlaceholder marks empty network graph results
	GetNetworkEmptyPlaceholder = "__EMPTY_NETWORK__"
	// GetNetworkEmptyTTL is the TTL for empty network results
	GetNetworkEmptyTTL = 2 * time.Minute

	// GetPathCachePrefix is the prefix for get path cache keys
	GetPathCachePrefix = "network:path:ids:"
	// GetPathCacheTTL is the TTL for path query cache
	GetPathCacheTTL = 15 * time.Minute // 路径查询缓存时间可以稍长
	// GetPathEmptyPlaceholder marks empty path results
	GetPathEmptyPlaceholder = "__EMPTY_PATH__"
	// GetPathEmptyTTL is the TTL for empty path results
	GetPathEmptyTTL = 2 * time.Minute

	// Expose prefixes for testing cleanup (Added)
	NodeCachePrefix          = "node:"
	RelationCachePrefix      = "relation:"
	NodeRelationsCachePrefix = "nodrels:"
)

// Define repository-level errors
var (
	ErrInvalidDepth = errors.New("repo: invalid depth value")
)

// neo4jNodeRepo 实现了 NodeRepository 接口
type neo4jNodeRepo struct {
	driver       neo4j.DriverWithContext
	nodeDAL      neo4jdal.NodeDAL
	cache        cache.NodeAndByteCache
	relationRepo RelationRepository
	// 添加配置字段
	defaultNodeTTL          time.Duration
	searchNodesTTL          time.Duration
	getNetworkTTL           time.Duration
	getPathTTL              time.Duration
	getNetworkMaxDepth      int
	getPathMaxDepth         int
	getPathMaxDepthLimit    int
	searchNodesDefaultLimit int
	logger                  *zap.Logger
}

// NewNodeRepository 创建一个新的 NodeRepository 实例
// 依赖注入 Neo4j 驱动、Node DAL 实现、组合缓存实现和 RelationRepository 实现
// 添加配置参数
func NewNodeRepository(
	driver neo4j.DriverWithContext,
	nodeDAL neo4jdal.NodeDAL,
	cache cache.NodeAndByteCache,
	relationRepo RelationRepository,
	defaultNodeTTLSeconds int,
	searchNodesTTLSeconds int,
	getNetworkTTLSeconds int,
	getPathTTLSeconds int,
	getNetworkMaxDepth int,
	getPathMaxDepth int,
	getPathMaxDepthLimit int,
	searchNodesDefaultLimit int,
	logger *zap.Logger,
) NodeRepository {
	return &neo4jNodeRepo{
		driver:       driver,
		nodeDAL:      nodeDAL,
		cache:        cache,
		relationRepo: relationRepo,
		// 将秒转换为 time.Duration
		defaultNodeTTL:          time.Duration(defaultNodeTTLSeconds) * time.Second,
		searchNodesTTL:          time.Duration(searchNodesTTLSeconds) * time.Second,
		getNetworkTTL:           time.Duration(getNetworkTTLSeconds) * time.Second,
		getPathTTL:              time.Duration(getPathTTLSeconds) * time.Second,
		getNetworkMaxDepth:      getNetworkMaxDepth,
		getPathMaxDepth:         getPathMaxDepth,
		getPathMaxDepthLimit:    getPathMaxDepthLimit,
		searchNodesDefaultLimit: searchNodesDefaultLimit,
		logger:                  logger,
	}
}

// CreateNode 在 Neo4j 中创建一个新节点
// 读旁路Read Aside 这种模式的核心思想是"读时填充缓存"。
// 在读取数据时（GetNode）发现缓存未命中，然后从数据库加载并回填到缓存中。CreateNode 属于写操作
// 所以createNode操作就不用处理缓存了
func (r *neo4jNodeRepo) CreateNode(ctx context.Context, req *network.CreateNodeRequest) (*network.Node, error) {
	session := r.driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)

	// 1. 生成唯一业务 ID
	nodeID := uuid.NewString()

	// 2. 构建节点属性 Map
	properties := map[string]any{
		"id":         nodeID,
		"name":       req.Name,
		"created_at": time.Now().UTC(),
		"updated_at": time.Now().UTC(),
	}
	// 处理可选字段
	if req.Avatar != nil {
		properties["avatar"] = *req.Avatar
	}
	if req.Profession != nil {
		properties["profession"] = *req.Profession
	}
	// 合并自定义属性，避免覆盖核心属性
	if req.Properties != nil {
		for k, v := range req.Properties {
			if _, exists := properties[k]; !exists {
				properties[k] = v
			}
		}
	}

	// 3. 调用 DAL 层执行数据库操作
	dbNode, err := r.nodeDAL.ExecCreateNode(ctx, session, req.Type, properties)
	if err != nil {
		return nil, fmt.Errorf("repo: 调用 DAL 创建节点失败: %w", err)
	}

	// 4. 将 DAL 返回的 dbtype.Node 映射为业务模型 network.Node
	// 注意：dbNode 可能不直接包含所有属性，映射函数需要处理
	// 暂时假设 mapDbNodeToThriftNode 能正确处理
	return mapDbNodeToThriftNode(dbNode, req.Type), nil
}

// GetNode 通过 ID 获取节点，应用 Read-Aside 缓存策略
func (r *neo4jNodeRepo) GetNode(ctx context.Context, id string) (*network.Node, error) {

	// 1. 尝试从缓存获取
	if r.cache != nil { // 检查缓存是否已配置
		cachedNode, err := r.cache.GetNode(ctx, id)
		if err == nil {
			r.logger.Debug("Repo: GetNode cache hit", zap.String("id", id))
			return cachedNode, nil
		} else if errors.Is(err, cache.ErrNilValue) {
			r.logger.Debug("Repo: GetNode cache hit with nil value", zap.String("id", id))
			return nil, err // 返回 cache.ErrNilValue 或 repo 层的 NotFound 错误
		} else if !errors.Is(err, cache.ErrNotFound) {
			r.logger.Warn("Repo: 缓存获取节点失败", zap.String("id", id), zap.Error(err))
			// 缓存出错，继续尝试从数据库获取，但要记录错误
		}
		// 如果是 cache.ErrNotFound，则继续执行数据库查询
	}

	// 2. 从数据库获取 (缓存未命中或缓存读取失败)
	r.logger.Debug("Repo: GetNode cache miss, querying database", zap.String("id", id))
	session := r.driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
	defer session.Close(ctx)

	dbNode, labels, err := r.nodeDAL.ExecGetNodeByID(ctx, session, id)
	if err != nil {
		// 处理 DAL 返回的特定错误，例如未找到
		if isNotFoundError(err) { // 使用辅助函数检查错误
			// 数据库未找到，尝试缓存空值
			if r.cache != nil {
				setErr := r.cache.SetNode(ctx, id, nil, cache.NilValueTTL) // 缓存空值
				if setErr != nil {
					r.logger.Warn("Repo: 缓存设置空值节点失败", zap.String("id", id), zap.Error(setErr))
				}
			}
			return nil, err // 直接透传 DB 的 Not Found 错误
		}
		// 其他数据库错误
		return nil, fmt.Errorf("repo: 调用 DAL 获取节点失败: %w", err)
	}

	// 3. 将 DAL 返回的 dbtype.Node 映射为业务模型 network.Node
	nodeType, ok := labelToNodeType(labels) // 使用辅助函数推断类型
	if !ok {
		// 如果无法从标签推断出已知的 NodeType，记录警告或返回错误
		r.logger.Warn("Repo: 无法识别节点 的标签", zap.String("id", id), zap.Strings("labels", labels))
		// 决定如何处理：返回错误、使用默认值、或尝试其他逻辑
		return nil, fmt.Errorf("repo: 无法识别的节点类型标签 %v", labels)
	}
	resultNode := mapDbNodeToThriftNode(dbNode, nodeType) // 使用辅助函数映射

	// 4. 存入缓存 (数据库获取成功)
	if r.cache != nil && resultNode != nil {
		// 使用配置的 TTL
		setErr := r.cache.SetNode(ctx, id, resultNode, r.defaultNodeTTL)
		if setErr != nil {
			r.logger.Warn("Repo: 缓存设置节点失败", zap.String("id", id), zap.Error(setErr))
		}
	}

	return resultNode, nil
}

// UpdateNode 更新节点属性，应用 Write Invalidation 缓存策略
func (r *neo4jNodeRepo) UpdateNode(ctx context.Context, req *network.UpdateNodeRequest) (*network.Node, error) {
	session := r.driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)

	// 1. 构建需要更新的属性 Map
	updates := map[string]any{
		"updated_at": time.Now().UTC(), // 总是更新 updated_at
	}
	// 处理可选更新字段
	if req.Name != nil {
		updates["name"] = *req.Name
	}
	if req.Avatar != nil {
		updates["avatar"] = *req.Avatar
	}
	if req.Profession != nil {
		updates["profession"] = *req.Profession
	}
	if req.Properties != nil {
		for k, v := range req.Properties {
			// 确保不覆盖核心属性或时间戳
			if k != "id" && k != "created_at" && k != "updated_at" {
				updates[k] = v
			}
		}
	}
	// 注意：如果需要支持删除属性，请求结构体需要增加字段，例如 `RemoveProperties []string`

	// 2. 调用 DAL 层执行更新
	dbNode, labels, err := r.nodeDAL.ExecUpdateNode(ctx, session, req.ID, updates)
	if err != nil {
		if isNotFoundError(err) {
			return nil, err // 透传 Not Found
		}
		return nil, fmt.Errorf("repo: 调用 DAL 更新节点失败: %w", err)
	}

	// 3. 映射结果
	nodeType, ok := labelToNodeType(labels)
	if !ok {
		r.logger.Warn("Repo: 更新后无法识别节点 的标签", zap.String("id", req.ID), zap.Strings("labels", labels))
		return nil, fmt.Errorf("repo: 更新后无法识别的节点类型标签 %v", labels)
	}
	updatedNode := mapDbNodeToThriftNode(dbNode, nodeType)

	// 4. 使缓存失效 (数据库操作成功后)
	if r.cache != nil { // 检查缓存是否已配置
		delErr := r.cache.DeleteNode(ctx, req.ID)
		if delErr != nil && !errors.Is(delErr, cache.ErrNotFound) { // 忽略 NotFound 错误
			// 删除缓存失败通常记录警告
			r.logger.Warn("Repo: 缓存删除节点失败", zap.String("id", req.ID), zap.Error(delErr))
		}
	}

	return updatedNode, nil
}

// DeleteNode 删除节点，应用 Write Invalidation 缓存策略
func (r *neo4jNodeRepo) DeleteNode(ctx context.Context, id string) error {
	session := r.driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)

	// 1. 调用 DAL 层执行删除
	err := r.nodeDAL.ExecDeleteNode(ctx, session, id)
	if err != nil {
		if isNotFoundError(err) {
			// 如果 DB 中本来就不存在，对应的缓存也应该删除（或已过期）
			// 所以即使是 NotFound，我们仍然尝试删除缓存
		} else {
			// 其他数据库错误
			return fmt.Errorf("repo: 调用 DAL 删除节点失败: %w", err)
		}
	}

	// 2. 使缓存失效 (无论 DB 操作是否 NotFound，都尝试删除)
	if r.cache != nil { // 检查缓存是否已配置
		delErr := r.cache.DeleteNode(ctx, id)
		if delErr != nil && !errors.Is(delErr, cache.ErrNotFound) { // 忽略 NotFound 错误
			// 删除缓存失败通常记录警告
			r.logger.Warn("Repo: 缓存删除节点失败", zap.String("id", id), zap.Error(delErr))
		}
	}

	// 如果原始错误是 NotFound，则透传它
	if isNotFoundError(err) {
		return err
	}

	return nil // DB 删除成功（或本来就不存在）且尝试删除缓存后返回 nil
}

// searchNodesCacheValue 定义了搜索结果缓存的结构
type searchNodesCacheValue struct {
	NodeIDs []string `json:"node_ids"`
	Total   int32    `json:"total"`
}

// generateSearchNodesCacheKey 生成搜索节点的缓存键
func generateSearchNodesCacheKey(req *network.SearchNodesRequest) string {
	// 1. 对 criteria map 的键进行排序
	keys := make([]string, 0, len(req.Criteria))
	for k := range req.Criteria {
		keys = append(keys, k)
	}
	sort.Strings(keys) // 确保键的顺序一致

	// 2. 构建规范化的 criteria 字符串 (例如: key1=value1|key2=value2)
	var criteriaBuilder strings.Builder
	for i, k := range keys {
		if i > 0 {
			criteriaBuilder.WriteString("|") // 使用分隔符
		}
		criteriaBuilder.WriteString(k)
		criteriaBuilder.WriteString("=")
		criteriaBuilder.WriteString(req.Criteria[k])
	}
	criteriaStr := criteriaBuilder.String()

	// 3. 使用 SHA1 哈希 criteria 字符串
	hasher := sha1.New()
	hasher.Write([]byte(criteriaStr))
	criteriaHash := hex.EncodeToString(hasher.Sum(nil))

	// 4. 处理可能为 nil 的 Limit 和 Offset
	var limitVal, offsetVal int32
	if req.Limit != nil {
		limitVal = *req.Limit
	}
	if req.Offset != nil {
		offsetVal = *req.Offset
	}

	// 5. NodeType 也是必需的
	var nodeTypeStr string
	if req.Type != nil { // 检查 Type 是否为 nil
		nodeTypeStr = req.Type.String()
	} else {
		nodeTypeStr = "ANY" // 或者其他默认值
	}

	// 6. 格式: prefix:criteria_hash:type:limit:offset
	return fmt.Sprintf("%s%s:%s:%d:%d", SearchNodesCachePrefix, criteriaHash, nodeTypeStr, limitVal, offsetVal)
}

// SearchNodes 搜索节点 (带缓存)
func (r *neo4jNodeRepo) SearchNodes(ctx context.Context, req *network.SearchNodesRequest) ([]*network.Node, int32, error) {
	// 0. 检查缓存是否可用
	if r.cache == nil {
		r.logger.Warn("Repo: Cache 未初始化，跳过 SearchNodes 缓存")
		return r.searchNodesDirect(ctx, req)
	}

	// 1. 生成缓存键
	cacheKey := generateSearchNodesCacheKey(req)

	// 2. 尝试从缓存获取
	cachedData, err := r.cache.Get(ctx, cacheKey)
	if err == nil { // 缓存命中
		// 2.0 检查是否是空结果标记
		if bytes.Equal(cachedData, []byte(SearchEmptyPlaceholder)) {
			r.logger.Info("Repo: SearchNodes 缓存命中空标记", zap.String("cacheKey", cacheKey))
			return []*network.Node{}, 0, nil // 返回空结果
		}

		// 2.1 尝试解析缓存的 ID 列表和总数
		var cachedValue searchNodesCacheValue
		if err := json.NewDecoder(bytes.NewReader(cachedData)).Decode(&cachedValue); err == nil {
			// 2.1 缓存命中且解析成功
			r.logger.Info("Repo: SearchNodes 缓存命中", zap.String("cacheKey", cacheKey))
			resultNodes := make([]*network.Node, 0, len(cachedValue.NodeIDs))
			for _, nodeID := range cachedValue.NodeIDs {
				// 2.2 使用 GetNode 获取节点详情 (GetNode 会处理自己的缓存)
				node, getNodeErr := r.GetNode(ctx, nodeID) // 注意：这里可能需要处理 GetNode 返回的错误
				if getNodeErr != nil {
					// 如果节点未找到（可能在缓存结果生成后被删除），可以选择跳过或记录日志
					if errors.Is(getNodeErr, cache.ErrNotFound) || isNotFoundError(getNodeErr) {
						r.logger.Warn("Repo: SearchNodes 缓存命中，但 GetNode 未找到节点 (可能已被删除)", zap.String("nodeID", nodeID))
						continue
					}
					// 其他 GetNode 错误，可能需要中断或返回错误
					r.logger.Error("Repo: SearchNodes 缓存命中，但 GetNode 失败", zap.String("nodeID", nodeID), zap.Error(getNodeErr))
					// 暂定策略：跳过此节点，继续处理其他节点
					continue
				}
				if node != nil { // 确保 GetNode 返回了有效的节点
					resultNodes = append(resultNodes, node)
				}
			}
			return resultNodes, cachedValue.Total, nil
		}
		// 缓存数据解析失败，当作缓存未命中处理
		r.logger.Error("Repo: SearchNodes 缓存数据解析失败", zap.String("cacheKey", cacheKey), zap.Error(err))
	} else if !errors.Is(err, cache.ErrNotFound) {
		// 非 "Not Found" 的缓存读取错误，记录日志但继续执行数据库查询
		r.logger.Error("Repo: SearchNodes 缓存读取失败", zap.String("cacheKey", cacheKey), zap.Error(err))
	} else {
		// 缓存未命中 (cache.ErrNotFound)
		r.logger.Info("Repo: SearchNodes 缓存未命中", zap.String("cacheKey", cacheKey))
	}

	// 3. 缓存未命中或出错，直接查询数据库
	resultNodes, total, err := r.searchNodesDirect(ctx, req)
	if err != nil {
		return nil, 0, err // 直接返回数据库查询错误
	}

	// 4. 缓存结果 (如果查询成功且有结果)
	if err == nil && len(resultNodes) > 0 {
		nodeIDs := make([]string, len(resultNodes))
		for i, node := range resultNodes {
			nodeIDs[i] = node.ID
		}

		cacheValue := searchNodesCacheValue{
			NodeIDs: nodeIDs,
			Total:   total,
		}

		var buffer bytes.Buffer
		if err := json.NewEncoder(&buffer).Encode(cacheValue); err == nil {
			// 使用配置的 TTL
			setErr := r.cache.Set(ctx, cacheKey, buffer.Bytes(), r.searchNodesTTL)
			if setErr != nil {
				r.logger.Error("Repo: SearchNodes 缓存写入失败", zap.String("cacheKey", cacheKey), zap.Error(setErr))
			} else {
				r.logger.Info("Repo: SearchNodes 结果已写入缓存", zap.String("cacheKey", cacheKey))
			}
		} else {
			r.logger.Error("Repo: SearchNodes 缓存值序列化失败", zap.String("cacheKey", cacheKey), zap.Error(err))
		}
	} else if err == nil && len(resultNodes) == 0 {
		// 4.1 缓存空结果标记 (使用 cache.NilValueTTL)
		setErr := r.cache.Set(ctx, cacheKey, []byte(SearchEmptyPlaceholder), cache.NilValueTTL)
		if setErr != nil {
			r.logger.Error("Repo: SearchNodes 缓存空标记写入失败", zap.String("cacheKey", cacheKey), zap.Error(setErr))
		} else {
			r.logger.Info("Repo: SearchNodes 空结果已写入缓存标记", zap.String("cacheKey", cacheKey))
		}
	}

	// 5. 返回从数据库获取的结果
	return resultNodes, total, nil
}

// searchNodesDirect 是实际执行数据库查询的逻辑 (从原 SearchNodes 提取)
func (r *neo4jNodeRepo) searchNodesDirect(ctx context.Context, req *network.SearchNodesRequest) ([]*network.Node, int32, error) {
	// --- 添加日志：打印接收到的请求参数 ---
	r.logger.Debug("Repo: searchNodesDirect called with Request",
		zap.Any("type", req.Type),
		zap.Any("limit", req.Limit),
		zap.Any("offset", req.Offset),
		zap.Any("criteria", req.Criteria))

	session := r.driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
	defer session.Close(ctx)

	// 直接使用传入的 criteria，如果为 nil 则初始化为空 map
	criteria := req.Criteria
	if criteria == nil {
		criteria = make(map[string]string)
	}

	// 处理分页参数 (使用配置的默认值)
	var limit, offset int64
	if req.Limit != nil {
		limit = int64(*req.Limit)
	} else {
		limit = int64(r.searchNodesDefaultLimit) // 使用配置的默认 Limit
	}
	if req.Offset != nil {
		offset = int64(*req.Offset)
	} else {
		offset = 0 // 默认偏移
	}

	// nodeType 从 req.Type 获取，已经是 *network.NodeType
	nodeTypePtr := req.Type

	// --- 添加日志：打印传递给 DAL 的参数 ---
	r.logger.Debug("Repo: Calling DAL ExecSearchNodes with",
		zap.Any("criteria", criteria),
		zap.Any("nodeType", nodeTypePtr),
		zap.Int64("limit", limit),
		zap.Int64("offset", offset))

	// 调用 DAL 层执行搜索
	// 确保 DAL 的 ExecSearchNodes 接受 map[string]string 作为 criteria 和 *network.NodeType 作为类型
	dbNodes, labelsList, total, err := r.nodeDAL.ExecSearchNodes(ctx, session, criteria, nodeTypePtr, limit, offset)
	if err != nil {
		// 注意：这里不需要检查 isNotFoundError，因为搜索本身找不到是正常情况，DAL应返回空列表和0 total
		// --- 添加日志：DAL 调用出错 ---
		r.logger.Error("Repo: DAL ExecSearchNodes failed", zap.Error(err))
		return nil, 0, fmt.Errorf("repo: 调用 DAL 搜索节点失败: %w", err)
	}

	// --- 添加日志：打印 DAL 返回结果 ---
	r.logger.Debug("Repo: DAL ExecSearchNodes returned", zap.Int("nodesCount", len(dbNodes)), zap.Int64("total", total))

	// 映射结果
	resultNodes := make([]*network.Node, 0, len(dbNodes))
	for i, dbNode := range dbNodes {
		labels := labelsList[i]
		nodeType, ok := labelToNodeType(labels) // 使用从DB获取的label来确定类型
		if !ok {
			nodeID := getStringProp(dbNode.Props, "id", "[未知ID]")
			r.logger.Warn("Repo: 搜索结果中无法识别节点 的标签", zap.String("nodeID", nodeID), zap.Strings("labels", labels))
			continue
		}
		thriftNode := mapDbNodeToThriftNode(dbNode, nodeType)
		if thriftNode != nil {
			resultNodes = append(resultNodes, thriftNode)
		}
	}

	return resultNodes, int32(total), nil
}

// getNetworkCacheValue 定义了 GetNetwork 结果缓存的结构
type getNetworkCacheValue struct {
	NodeIDs     []string `json:"node_ids"`
	RelationIDs []string `json:"relation_ids"`
}

// generateGetNetworkCacheKey 生成 GetNetwork 的缓存键
func generateGetNetworkCacheKey(req *network.GetNetworkRequest, maxDepth int32, limit, offset int64) string {
	// 1. 对 criteria map 的键进行排序
	criteriaKeys := make([]string, 0, len(req.StartNodeCriteria))
	for k := range req.StartNodeCriteria {
		criteriaKeys = append(criteriaKeys, k)
	}
	sort.Strings(criteriaKeys)

	// 2. 构建规范化的 criteria 字符串
	var criteriaBuilder strings.Builder
	for i, k := range criteriaKeys {
		if i > 0 {
			criteriaBuilder.WriteString("|")
		}
		criteriaBuilder.WriteString(k)
		criteriaBuilder.WriteString("=")
		criteriaBuilder.WriteString(req.StartNodeCriteria[k])
	}
	criteriaStr := criteriaBuilder.String()

	// 3. 对 relationTypes 进行排序和拼接
	relTypesStr := make([]string, 0, len(req.RelationTypes))
	if req.IsSetRelationTypes() {
		for _, rt := range req.RelationTypes {
			relTypesStr = append(relTypesStr, rt.String())
		}
	}
	sort.Strings(relTypesStr)
	relTypesKeyPart := strings.Join(relTypesStr, ",")

	// 4. 对 nodeTypes 进行排序和拼接
	nodeTypesStr := make([]string, 0, len(req.NodeTypes))
	if req.IsSetNodeTypes() {
		for _, nt := range req.NodeTypes {
			nodeTypesStr = append(nodeTypesStr, nt.String())
		}
	}
	sort.Strings(nodeTypesStr)
	nodeTypesKeyPart := strings.Join(nodeTypesStr, ",")

	// 5. 哈希组合键以避免过长
	hasher := sha1.New()
	hasher.Write([]byte(criteriaStr))
	hasher.Write([]byte(relTypesKeyPart))
	hasher.Write([]byte(nodeTypesKeyPart))
	combinedHash := hex.EncodeToString(hasher.Sum(nil))

	// 6. 格式: prefix:combined_hash:depth:limit:offset
	return fmt.Sprintf("%s%s:%d:%d:%d", GetNetworkCachePrefix, combinedHash, maxDepth, limit, offset)
}

// GetNetwork 获取网络图谱 (节点和关系)，带缓存
// TODO: 从config文件中读取maxDepth
func (r *neo4jNodeRepo) GetNetwork(ctx context.Context, req *network.GetNetworkRequest) ([]*network.Node, []*network.Relation, error) {
	// --- Handle Depth < 0 --- (New)
	if req.Depth < 0 {
		return nil, nil, ErrInvalidDepth // Return specific error for negative depth
	}

	// --- Handle Depth == 0 --- (New)
	if req.Depth == 0 {
		r.logger.Info("Repo: GetNetwork called with Depth 0, finding start nodes only.")
		// Directly find start nodes using SearchNodes logic
		searchReq := &network.SearchNodesRequest{
			Criteria: req.StartNodeCriteria,
			Type:     nil,                                      // SearchNodes DAL/Repo handles type matching if needed based on criteria or labels
			Limit:    func(i int32) *int32 { return &i }(1000), // Use a reasonable limit for start nodes
			Offset:   func(i int32) *int32 { return &i }(0),
		}
		// If specific node types are provided in GetNetwork request, set them in SearchNodes request
		if req.IsSetNodeTypes() && len(req.NodeTypes) > 0 {
			// Note: SearchNodes expects *NodeType, and currently only supports one.
			// We'll just use the first one specified if multiple are given for depth=0 case.
			// A more robust solution might involve modifying SearchNodes or using a different DAL method.
			if len(req.NodeTypes) > 1 {
				r.logger.Warn("Repo: GetNetwork with Depth 0 received multiple NodeTypes, using only the first for start node search.",
					zap.String("firstNodeType", req.NodeTypes[0].String()))
			}
			typePtr := req.NodeTypes[0] // Take the first type
			searchReq.Type = &typePtr
		}

		// Call SearchNodes to find the start nodes.
		// We ignore the 'total' count here.
		startNodes, _, err := r.SearchNodes(ctx, searchReq)
		if err != nil {
			return nil, nil, fmt.Errorf("repo: failed to find start nodes for GetNetwork(Depth 0): %w", err)
		}
		// Return only the found start nodes and an empty relation slice.
		return startNodes, []*network.Relation{}, nil
	}

	// --- Handle Depth > 0 (Existing Logic) ---
	// 1. 处理参数和计算默认值
	var maxDepth int32 = req.Depth // Use the positive depth from the request
	if maxDepth > 5 {              // 仍然限制最大深度
		r.logger.Warn("Repo: Requested GetNetwork depth too high, resetting to 5",
			zap.Int32("requestedDepth", req.Depth),
			zap.Int32("maxDepth", 5))
		maxDepth = 5 // Reset to max allowed depth (e.g., 5)
	}

	var limit int64 = 100 // 默认限制
	var offset int64 = 0  // 默认偏移

	// 2. 检查缓存和 RelationRepository 是否可用
	if r.cache == nil || r.relationRepo == nil {
		r.logger.Warn("Repo: GetNetwork cache or relationRepo not initialized, skipping cache.")
		return r.getNetworkDirect(ctx, req, maxDepth, limit, offset)
	}

	// 3. 生成缓存键
	cacheKey := generateGetNetworkCacheKey(req, maxDepth, limit, offset)

	// 4. 尝试从缓存获取
	cachedData, err := r.cache.Get(ctx, cacheKey)
	if err == nil { // 缓存命中
		// 4.1 检查空标记
		if bytes.Equal(cachedData, []byte(GetNetworkEmptyPlaceholder)) {
			r.logger.Info("Repo: GetNetwork cache hit empty placeholder", zap.String("cacheKey", cacheKey))
			return []*network.Node{}, []*network.Relation{}, nil
		}

		// 4.2 解析缓存的 ID 列表
		var cachedValue getNetworkCacheValue
		if err := json.NewDecoder(bytes.NewReader(cachedData)).Decode(&cachedValue); err == nil {
			r.logger.Info("Repo: GetNetwork cache hit, fetching details", zap.String("cacheKey", cacheKey))
			resultNodes := make([]*network.Node, 0, len(cachedValue.NodeIDs))
			resultRelations := make([]*network.Relation, 0, len(cachedValue.RelationIDs))
			failedFetches := 0

			// 4.3 使用 GetNode 获取节点
			for _, nodeID := range cachedValue.NodeIDs {
				node, getNodeErr := r.GetNode(ctx, nodeID)
				if getNodeErr != nil {
					failedFetches++
					if errors.Is(getNodeErr, cache.ErrNotFound) || isNotFoundError(getNodeErr) || errors.Is(getNodeErr, cache.ErrNilValue) {
						r.logger.Warn("Repo: GetNetwork cache hit, but GetNode couldn't find node (may be deleted or nil cached)", zap.String("nodeID", nodeID))
					} else {
						r.logger.Error("Repo: GetNetwork cache hit, but GetNode failed", zap.String("nodeID", nodeID), zap.Error(getNodeErr))
					}
					continue // 跳过获取失败的节点
				}
				if node != nil {
					resultNodes = append(resultNodes, node)
				}
			}

			// 4.4 使用 GetRelation 获取关系
			for _, relationID := range cachedValue.RelationIDs {
				relation, getRelErr := r.relationRepo.GetRelation(ctx, relationID)
				if getRelErr != nil {
					failedFetches++
					if errors.Is(getRelErr, cache.ErrNotFound) || isNotFoundError(getRelErr) || errors.Is(getRelErr, cache.ErrNilValue) {
						r.logger.Warn("Repo: GetNetwork cache hit, but GetRelation couldn't find relation (may be deleted or nil cached)", zap.String("relationID", relationID))
					} else {
						r.logger.Error("Repo: GetNetwork cache hit, but GetRelation failed", zap.String("relationID", relationID), zap.Error(getRelErr))
					}
					continue // 跳过获取失败的关系
				}
				if relation != nil {
					resultRelations = append(resultRelations, relation)
				}
			}

			if failedFetches > 0 {
				r.logger.Warn("Repo: GetNetwork cache hit, but nodes/relations failed to fetch from detail cache/DB",
					zap.Int("failedFetches", failedFetches),
					zap.String("cacheKey", cacheKey))
			}
			return resultNodes, resultRelations, nil
		}
		// 缓存数据解析失败，当作未命中
		r.logger.Error("Repo: GetNetwork cache data decode failed", zap.String("cacheKey", cacheKey), zap.Error(err))

	} else if !errors.Is(err, cache.ErrNotFound) {
		// 非 NotFound 的缓存错误，记录并继续查库
		r.logger.Error("Repo: GetNetwork cache get failed", zap.String("cacheKey", cacheKey), zap.Error(err))
	} else {
		r.logger.Info("Repo: GetNetwork cache miss", zap.String("cacheKey", cacheKey))
	}

	// 5. 缓存未命中或出错，直接查询数据库
	resultNodes, resultRelations, dbNodes, dbRelations, err := r.getNetworkDirectAndRaw(ctx, req, maxDepth, limit, offset)
	if err != nil {
		// 如果 DAL 层出错，不进行缓存，直接返回错误
		return nil, nil, err
	}

	// 6. 缓存结果 (只有在 DAL 没有错误时才执行)
	// 判断是否真的有结果 (使用 getNetworkDirectAndRaw 返回的映射后结果)
	isEmptyResult := len(resultNodes) == 0 && len(resultRelations) == 0

	if isEmptyResult {
		// 6.1 缓存空标记
		setErr := r.cache.Set(ctx, cacheKey, []byte(GetNetworkEmptyPlaceholder), cache.NilValueTTL)
		if setErr != nil {
			r.logger.Error("Repo: GetNetwork cache set empty placeholder failed", zap.String("cacheKey", cacheKey), zap.Error(setErr))
		} else {
			r.logger.Info("Repo: GetNetwork set empty placeholder to cache", zap.String("cacheKey", cacheKey))
		}
	} else {
		// 6.2 提取 ID 并缓存实际结果
		//    使用 getNetworkDirectAndRaw 返回的原始 db* 变量来获取 ID
		nodeIDs := make([]string, 0, len(dbNodes))
		for _, dbNode := range dbNodes {
			if nodeID := getStringProp(dbNode.Props, "id", ""); nodeID != "" {
				nodeIDs = append(nodeIDs, nodeID)
			} else {
				r.logger.Error("Repo: GetNetwork DB result node missing 'id' property", zap.String("elementId", dbNode.ElementId))
				// 如果节点没有ID，无法缓存，可能返回错误或跳过缓存
				goto SkipCache // 跳到缓存步骤之后
			}
		}
		relationIDs := make([]string, 0, len(dbRelations))
		for _, dbRel := range dbRelations {
			if relID := getStringProp(dbRel.Props, "id", ""); relID != "" {
				relationIDs = append(relationIDs, relID)
			} else {
				r.logger.Error("Repo: GetNetwork DB result relation missing 'id' property", zap.String("elementId", dbRel.ElementId))
				// 如果关系没有ID，无法缓存
				goto SkipCache
			}
		}

		// 6.2 序列化并缓存
		cacheValue := getNetworkCacheValue{
			NodeIDs:     nodeIDs,
			RelationIDs: relationIDs,
		}
		var buffer bytes.Buffer
		if encErr := json.NewEncoder(&buffer).Encode(cacheValue); encErr == nil {
			setErr := r.cache.Set(ctx, cacheKey, buffer.Bytes(), r.getNetworkTTL)
			if setErr != nil {
				r.logger.Error("Repo: GetNetwork cache set failed", zap.String("cacheKey", cacheKey), zap.Error(setErr))
			} else {
				r.logger.Info("Repo: GetNetwork set data to cache", zap.String("cacheKey", cacheKey))
			}
		} else {
			r.logger.Error("Repo: GetNetwork cache value encode failed", zap.String("cacheKey", cacheKey), zap.Error(encErr))
		}
	}

SkipCache:
	// 7. 返回从数据库获取并映射的结果
	return resultNodes, resultRelations, nil
}

// getNetworkDirect 是实际执行数据库查询和映射的逻辑 (从原 GetNetwork 提取)
// 为了缓存，我们需要同时返回映射后的结果和原始的 DB 结果以提取 ID
// 因此创建一个新的内部函数 getNetworkDirectAndRaw
func (r *neo4jNodeRepo) getNetworkDirectAndRaw(ctx context.Context, req *network.GetNetworkRequest, maxDepth int32, limit, offset int64) (
	resultNodes []*network.Node,
	resultRelations []*network.Relation,
	dbNodes []dbtype.Node,
	dbRelations []dbtype.Relationship,
	err error,
) {
	session := r.driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
	defer session.Close(ctx)

	// 调用 DAL 层获取网络数据 - 使用新的参数
	dbNodes, dbRelations, err = r.nodeDAL.ExecGetNetwork(
		ctx,
		session,
		req.StartNodeCriteria, // 使用 StartNodeCriteria
		maxDepth,
		limit,
		offset,
		req.RelationTypes, // 传递 RelationTypes
		req.NodeTypes,     // 传递 NodeTypes
	)
	if err != nil {
		// GetNetwork 通常不认为"未找到匹配 profession 的节点"是错误，DAL 应返回空列表
		// 仅处理真正的执行错误
		err = fmt.Errorf("repo: 调用 DAL 获取网络失败: %w", err)
		return // 返回错误和空的 slices
	}

	// 如果结果集为空 (可能是原始为空)
	if len(dbNodes) == 0 && len(dbRelations) == 0 {
		resultNodes = []*network.Node{}
		resultRelations = []*network.Relation{}
		return // return empty slices and nil error
	}

	// 映射节点
	nodesMap := make(map[int64]*network.Node) // 使用 Neo4j 内部 ID 暂存以处理关系
	resultNodes = make([]*network.Node, 0, len(dbNodes))
	for _, dbNode := range dbNodes {
		nodeType, ok := labelToNodeType(dbNode.Labels)
		if !ok {
			r.logger.Warn("Repo: GetNetwork 中无法识别节点 的标签", zap.String("elementId", dbNode.ElementId), zap.Strings("labels", dbNode.Labels))
			continue
		}
		thriftNode := mapDbNodeToThriftNode(dbNode, nodeType)
		if thriftNode != nil {
			resultNodes = append(resultNodes, thriftNode)
			nodesMap[dbNode.Id] = thriftNode
		}
	}

	// 映射关系
	resultRelations = make([]*network.Relation, 0, len(dbRelations))
	for _, dbRel := range dbRelations {
		sourceNode, sourceExists := nodesMap[dbRel.StartId]
		targetNode, targetExists := nodesMap[dbRel.EndId]
		if !sourceExists || !targetExists {
			r.logger.Warn("Repo: GetNetwork 中关系 的源或目标节点不在返回的节点列表中",
				zap.String("elementId", dbRel.ElementId),
				zap.Int64("startId", dbRel.StartId),
				zap.Int64("endId", dbRel.EndId))
			continue
		}

		relType, ok := stringToRelationType(dbRel.Type)
		if !ok {
			r.logger.Warn("Repo: GetNetwork 中无法识别关系 的类型", zap.String("elementId", dbRel.ElementId), zap.String("type", dbRel.Type))
			continue
		}

		thriftRelation := mapDbRelationshipToThriftRelation(dbRel, relType, sourceNode.ID, targetNode.ID)
		if thriftRelation != nil {
			resultRelations = append(resultRelations, thriftRelation)
		}
	}

	return // 返回映射结果、原始 DB 结果和 nil 错误
}

// getNetworkDirect (旧版，仅用于在缓存未初始化时调用)
func (r *neo4jNodeRepo) getNetworkDirect(ctx context.Context, req *network.GetNetworkRequest, maxDepth int32, limit, offset int64) ([]*network.Node, []*network.Relation, error) {
	rn, rr, _, _, err := r.getNetworkDirectAndRaw(ctx, req, maxDepth, limit, offset)
	return rn, rr, err
}

// getPathCacheValue 定义了 GetPath 结果缓存的结构 (保持顺序)
type getPathCacheValue struct {
	NodeIDs     []string `json:"node_ids"`
	RelationIDs []string `json:"relation_ids"`
}

// generateGetPathCacheKey 生成 GetPath 的缓存键
func generateGetPathCacheKey(req *network.GetPathRequest, maxDepth int32, relationTypesStr []string) string {
	// 对关系类型字符串进行排序，确保顺序无关性
	sortedTypes := make([]string, len(relationTypesStr))
	copy(sortedTypes, relationTypesStr)
	sort.Strings(sortedTypes)
	typesKeyPart := strings.Join(sortedTypes, ",")

	// 对类型部分进行哈希，避免键过长
	hasher := sha1.New()
	hasher.Write([]byte(typesKeyPart))
	typesHash := hex.EncodeToString(hasher.Sum(nil))

	// 格式: prefix:sourceID:targetID:maxDepth:typesHash
	return fmt.Sprintf("%s%s:%s:%d:%s", GetPathCachePrefix, req.SourceID, req.TargetID, maxDepth, typesHash)
}

// GetPath 获取两个节点之间的最短路径 (带缓存)
// TODO:从config文件中读取maxDepth
func (r *neo4jNodeRepo) GetPath(ctx context.Context, req *network.GetPathRequest) ([]*network.Node, []*network.Relation, error) {
	// 1. 处理参数 (与缓存键生成相关)
	var maxDepth int32 = 3
	if req.IsSetMaxDepth() {
		maxDepth = *req.MaxDepth
		if maxDepth <= 0 || maxDepth > 10 {
			maxDepth = 3
		}
	}
	var relationTypesStr []string
	if req.IsSetTypes() && len(req.Types) > 0 {
		relationTypesStr = make([]string, 0, len(req.Types))
		for _, rt := range req.Types {
			relationTypesStr = append(relationTypesStr, rt.String())
		}
	}

	// 2. 检查缓存和 RelationRepository 是否可用
	if r.cache == nil || r.relationRepo == nil {
		r.logger.Warn("Repo: GetPath cache or relationRepo not initialized, skipping cache.")
		return r.getPathDirect(ctx, req, maxDepth, relationTypesStr)
	}

	// 3. 生成缓存键
	cacheKey := generateGetPathCacheKey(req, maxDepth, relationTypesStr)

	// 4. 尝试从缓存获取
	cachedData, err := r.cache.Get(ctx, cacheKey)
	if err == nil { // 缓存命中
		// 4.1 检查空标记
		if bytes.Equal(cachedData, []byte(GetPathEmptyPlaceholder)) {
			r.logger.Info("Repo: GetPath cache hit empty placeholder", zap.String("cacheKey", cacheKey))
			// 路径未找到，根据接口约定，可能需要返回特定错误而非空列表
			// return []*network.Node{}, []*network.Relation{}, nil // 或者返回原始的 not found 错误?
			return nil, nil, fmt.Errorf("repo: path not found (cached empty)") // 返回一个表示未找到的错误
		}

		// 4.2 解析缓存的 ID 列表
		var cachedValue getPathCacheValue
		if err := json.NewDecoder(bytes.NewReader(cachedData)).Decode(&cachedValue); err == nil {
			r.logger.Info("Repo: GetPath cache hit, fetching details", zap.String("cacheKey", cacheKey))
			resultNodes := make([]*network.Node, 0, len(cachedValue.NodeIDs))
			resultRelations := make([]*network.Relation, 0, len(cachedValue.RelationIDs))
			failedFetches := 0

			// 4.3 按顺序获取节点
			for _, nodeID := range cachedValue.NodeIDs {
				node, getNodeErr := r.GetNode(ctx, nodeID)
				if getNodeErr != nil {
					failedFetches++
					if errors.Is(getNodeErr, cache.ErrNotFound) || isNotFoundError(getNodeErr) || errors.Is(getNodeErr, cache.ErrNilValue) {
						r.logger.Warn("Repo: GetPath cache hit, but GetNode couldn't find node in path", zap.String("nodeID", nodeID))
					} else {
						r.logger.Error("Repo: GetPath cache hit, but GetNode failed", zap.String("nodeID", nodeID), zap.Error(getNodeErr))
					}
					// 如果路径中的节点获取失败，整个路径可能无效，中断处理
					return nil, nil, fmt.Errorf("repo: failed to reconstruct path from cache, node %s fetch failed: %w", nodeID, getNodeErr)
				}
				if node != nil {
					resultNodes = append(resultNodes, node)
				}
			}

			// 4.4 按顺序获取关系
			for _, relationID := range cachedValue.RelationIDs {
				relation, getRelErr := r.relationRepo.GetRelation(ctx, relationID)
				if getRelErr != nil {
					failedFetches++
					if errors.Is(getRelErr, cache.ErrNotFound) || isNotFoundError(getRelErr) || errors.Is(getRelErr, cache.ErrNilValue) {
						r.logger.Warn("Repo: GetPath cache hit, but GetRelation couldn't find relation in path", zap.String("relationID", relationID))
					} else {
						r.logger.Error("Repo: GetPath cache hit, but GetRelation failed", zap.String("relationID", relationID), zap.Error(getRelErr))
					}
					// 如果路径中的关系获取失败，整个路径可能无效，中断处理
					return nil, nil, fmt.Errorf("repo: failed to reconstruct path from cache, relation %s fetch failed: %w", relationID, getRelErr)
				}
				if relation != nil {
					resultRelations = append(resultRelations, relation)
				}
			}

			return resultNodes, resultRelations, nil
		}
		// 缓存数据解析失败，当作未命中
		r.logger.Error("Repo: GetPath cache data decode failed", zap.String("cacheKey", cacheKey), zap.Error(err))
	} else if !errors.Is(err, cache.ErrNotFound) {
		// 非 NotFound 的缓存错误，记录并继续查库
		r.logger.Error("Repo: GetPath cache get failed", zap.String("cacheKey", cacheKey), zap.Error(err))
	} else {
		r.logger.Info("Repo: GetPath cache miss", zap.String("cacheKey", cacheKey))
	}

	// 5. 缓存未命中或出错，直接查询数据库
	resultNodes, resultRelations, dbNodes, dbRelations, err := r.getPathDirectAndRaw(ctx, req, maxDepth, relationTypesStr)
	if err != nil {
		// 5.1 处理错误和缓存空占位符
		if isNotFoundError(err) { // 确保 isNotFoundError 能识别 DAL 的错误和 Repo 包装的错误
			r.logger.Info("Repo: GetPath query returned not found, caching empty placeholder.",
				zap.String("cacheKey", cacheKey),
				zap.Error(err)) // Log the original not found error
			setErr := r.cache.Set(ctx, cacheKey, []byte(GetPathEmptyPlaceholder), cache.NilValueTTL)
			if setErr != nil {
				r.logger.Error("Repo: GetPath cache set empty placeholder failed", zap.String("cacheKey", cacheKey), zap.Error(setErr))
			} else {
				r.logger.Info("Repo: GetPath set empty placeholder to cache", zap.String("cacheKey", cacheKey))
			}
			// 返回原始的 Not Found 错误给调用者
			return nil, nil, err
		} else {
			// 其他数据库错误，直接返回，不缓存占位符
			r.logger.Error("Repo: GetPath query failed", zap.String("cacheKey", cacheKey), zap.Error(err))
			return nil, nil, err
		}
	}

	// 6. 缓存结果 (查询成功且找到路径)
	// 注意：之前的逻辑是 (len(dbNodes) > 0 || len(dbRelations) > 0)，对于只有单个节点路径可能不适用。
	// 只要 err == nil 并且不是 not found，就认为路径找到了（即使只有一个节点）
	if len(dbNodes) > 0 { // 至少要有一个节点才算有效路径
		// 6.1 按顺序提取 ID
		nodeIDs := make([]string, 0, len(dbNodes))
		for _, dbNode := range dbNodes {
			if nodeID := getStringProp(dbNode.Props, "id", ""); nodeID != "" {
				nodeIDs = append(nodeIDs, nodeID)
			} else {
				r.logger.Error("Repo: GetPath DB result node missing 'id' property", zap.String("elementId", dbNode.ElementId))
				// 如果节点没有ID，无法缓存，可能返回错误或跳过缓存
				goto SkipCache // 跳到缓存步骤之后
			}
		}
		relationIDs := make([]string, 0, len(dbRelations))
		for _, dbRel := range dbRelations {
			if relID := getStringProp(dbRel.Props, "id", ""); relID != "" {
				relationIDs = append(relationIDs, relID)
			} else {
				r.logger.Error("Repo: GetPath DB result relation missing 'id' property", zap.String("elementId", dbRel.ElementId))
				// 如果关系没有ID，无法缓存
				goto SkipCache
			}
		}

		// 6.2 序列化并缓存
		cacheValue := getPathCacheValue{
			NodeIDs:     nodeIDs,
			RelationIDs: relationIDs,
		}
		var buffer bytes.Buffer
		if encErr := json.NewEncoder(&buffer).Encode(cacheValue); encErr == nil {
			setErr := r.cache.Set(ctx, cacheKey, buffer.Bytes(), r.getPathTTL)
			if setErr != nil {
				r.logger.Error("Repo: GetPath cache set failed", zap.String("cacheKey", cacheKey), zap.Error(setErr))
			} else {
				r.logger.Info("Repo: GetPath set data to cache", zap.String("cacheKey", cacheKey))
			}
		} else {
			r.logger.Error("Repo: GetPath cache value encode failed", zap.String("cacheKey", cacheKey), zap.Error(encErr))
		}
	}

SkipCache:
	// 7. 返回从数据库获取并映射的结果
	return resultNodes, resultRelations, nil
}

// getPathDirectAndRaw 是实际执行 GetPath 数据库查询和映射的逻辑
func (r *neo4jNodeRepo) getPathDirectAndRaw(ctx context.Context, req *network.GetPathRequest, maxDepth int32, relationTypesStr []string) (
	resultNodes []*network.Node,
	resultRelations []*network.Relation,
	dbNodes []dbtype.Node,
	dbRelations []dbtype.Relationship,
	err error,
) {
	session := r.driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
	defer session.Close(ctx)

	// 调用 DAL 层获取路径数据
	dbNodes, dbRelations, err = r.nodeDAL.ExecGetPath(ctx, session, req.SourceID, req.TargetID, maxDepth, relationTypesStr)
	if err != nil {
		// 直接将 DAL 错误（包括 Not Found）向上传递
		// 在 GetPath 方法中处理 Not Found 的缓存逻辑和错误返回
		// 包装一下错误，以便上层识别来源
		if isNotFoundError(err) {
			// 保持原始错误类型，但添加上下文
			err = fmt.Errorf("repo: path not found between %s and %s (depth %d, types %v): %w", req.SourceID, req.TargetID, maxDepth, relationTypesStr, err)
		} else {
			err = fmt.Errorf("repo: 调用 DAL 获取路径失败: %w", err)
		}
		return // 返回错误和空的 slices
	}

	// 如果 DAL 层没有返回错误，但返回了空节点，也视为未找到路径
	if len(dbNodes) == 0 {
		// 显式返回一个可被 isNotFoundError 识别的错误
		// err = neo4jdal.ErrNotFound // Cannot use this as it's not exported
		err = fmt.Errorf("repo: path not found between %s and %s (empty result from DAL): %w", req.SourceID, req.TargetID, ErrPathNotFound)
		return // 返回错误和空的 slices
	}

	// 映射节点 (保持顺序)
	nodesMap := make(map[int64]*network.Node) // 仍然需要 Map 来查找关系端点
	resultNodes = make([]*network.Node, len(dbNodes))
	for i, dbNode := range dbNodes {
		nodeType, ok := labelToNodeType(dbNode.Labels)
		if !ok {
			r.logger.Warn("Repo: GetPath 中无法识别节点 的标签", zap.String("elementId", dbNode.ElementId), zap.Strings("labels", dbNode.Labels))
			err = fmt.Errorf("repo: 路径中节点类型未知 (%v)", dbNode.Labels)
			return // 映射失败，返回错误
		}
		thriftNode := mapDbNodeToThriftNode(dbNode, nodeType)
		if thriftNode == nil {
			err = fmt.Errorf("repo: 路径节点映射失败 (elementId: %s)", dbNode.ElementId)
			return // 映射失败
		}
		resultNodes[i] = thriftNode
		nodesMap[dbNode.Id] = thriftNode
	}

	// 映射关系 (保持顺序)
	resultRelations = make([]*network.Relation, len(dbRelations))
	for i, dbRel := range dbRelations {
		sourceNode, sourceExists := nodesMap[dbRel.StartId]
		targetNode, targetExists := nodesMap[dbRel.EndId]
		if !sourceExists || !targetExists {
			r.logger.Error("Repo: GetPath 中关系 的源或目标节点查找失败",
				zap.String("elementId", dbRel.ElementId),
				zap.Int64("startId", dbRel.StartId),
				zap.Int64("endId", dbRel.EndId))
			err = fmt.Errorf("repo: 路径中关系端点查找失败")
			return // 映射失败
		}

		relType, ok := stringToRelationType(dbRel.Type)
		if !ok {
			r.logger.Warn("Repo: GetPath 中无法识别关系 的类型", zap.String("elementId", dbRel.ElementId), zap.String("type", dbRel.Type))
			err = fmt.Errorf("repo: 路径中关系类型未知 (%s)", dbRel.Type)
			return // 映射失败
		}

		thriftRelation := mapDbRelationshipToThriftRelation(dbRel, relType, sourceNode.ID, targetNode.ID)
		if thriftRelation == nil {
			err = fmt.Errorf("repo: 路径关系映射失败 (elementId: %s)", dbRel.ElementId)
			return // 映射失败
		}
		resultRelations[i] = thriftRelation
	}

	return // 返回映射结果、原始 DB 结果和 nil 错误
}

// getPathDirect (旧版，仅用于在缓存未初始化时调用)
func (r *neo4jNodeRepo) getPathDirect(ctx context.Context, req *network.GetPathRequest, maxDepth int32, relationTypesStr []string) ([]*network.Node, []*network.Relation, error) {
	rn, rr, _, _, err := r.getPathDirectAndRaw(ctx, req, maxDepth, relationTypesStr)
	return rn, rr, err
}
