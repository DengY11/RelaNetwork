package neo4jrepo

import (
	"bytes"
	"context" // 用于错误处理
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors" // 用于缓存错误检查
	"fmt"    // 用于错误检查
	"time"

	"github.com/google/uuid"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/dbtype"

	"labelwall/biz/dal/neo4jdal"
	network "labelwall/biz/model/relationship/network"
	"labelwall/pkg/cache" // 引入缓存包
	"sort"
	"strings"
)

const (
	// 默认节点缓存时间
	defaultNodeTTL = 1 * time.Hour

	searchNodesCachePrefix = "search:nodes:ids:"
	searchNodesCacheTTL    = 5 * time.Minute // 搜索结果缓存 5 分钟
	// 空搜索结果标记及 TTL (较短，防止长时间缓存无效搜索)
	searchEmptyPlaceholder = "__EMPTY_SEARCH__"
	searchEmptyTTL         = 1 * time.Minute

	getNetworkCachePrefix = "network:graph:ids:"
	getNetworkCacheTTL    = 10 * time.Minute // 网络图谱缓存 10 分钟 (比搜索长一些)
	// 空网络图谱结果标记及 TTL
	getNetworkEmptyPlaceholder = "__EMPTY_NETWORK__"
	getNetworkEmptyTTL         = 2 * time.Minute

	getPathCachePrefix = "network:path:ids:"
	getPathCacheTTL    = 15 * time.Minute // 路径查询缓存时间可以稍长
	// 空路径结果标记及 TTL
	getPathEmptyPlaceholder = "__EMPTY_PATH__"
	getPathEmptyTTL         = 2 * time.Minute
)

// neo4jNodeRepo 实现了 NodeRepository 接口
type neo4jNodeRepo struct {
	driver       neo4j.DriverWithContext
	nodeDAL      neo4jdal.NodeDAL
	cache        cache.NodeAndByteCache
	relationRepo RelationRepository
}

// NewNodeRepository 创建一个新的 NodeRepository 实例
// 依赖注入 Neo4j 驱动、Node DAL 实现、组合缓存实现和 RelationRepository 实现
func NewNodeRepository(
	driver neo4j.DriverWithContext,
	nodeDAL neo4jdal.NodeDAL,
	cache cache.NodeAndByteCache,
	relationRepo RelationRepository,
) NodeRepository {
	return &neo4jNodeRepo{
		driver:       driver,
		nodeDAL:      nodeDAL,
		cache:        cache,
		relationRepo: relationRepo,
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
			// 缓存命中
			fmt.Printf("Repo: GetNode cache hit for id: %s\n", id) // TODO: 使用日志库
			return cachedNode, nil
		} else if errors.Is(err, cache.ErrNilValue) {
			// 缓存命中，但存的是空值 (表示 DB 中不存在)
			fmt.Printf("Repo: GetNode cache hit with nil value for id: %s\n", id) // TODO: 使用日志库
			return nil, err                                                       // 返回 cache.ErrNilValue 或 repo 层的 NotFound 错误
		} else if !errors.Is(err, cache.ErrNotFound) {
			// 缓存读取发生错误 (非 NotFound)
			fmt.Printf("WARN: Repo: 缓存获取节点失败 (id: %s): %v\n", id, err) // TODO: 使用日志库
			// 缓存出错，继续尝试从数据库获取，但要记录错误
		}
		// 如果是 cache.ErrNotFound，则继续执行数据库查询
	}

	// 2. 从数据库获取 (缓存未命中或缓存读取失败)
	fmt.Printf("Repo: GetNode cache miss for id: %s, querying database...\n", id) // TODO: 使用日志库
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
					fmt.Printf("WARN: Repo: 缓存设置空值节点失败 (id: %s): %v\n", id, setErr) // TODO: 使用日志库
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
		fmt.Printf("WARN: Repo: 无法识别节点 (id: %s) 的标签: %v\n", id, labels) // TODO: 使用日志库
		// 决定如何处理：返回错误、使用默认值、或尝试其他逻辑
		return nil, fmt.Errorf("repo: 无法识别的节点类型标签 %v", labels)
	}
	resultNode := mapDbNodeToThriftNode(dbNode, nodeType) // 使用辅助函数映射

	// 4. 存入缓存 (数据库获取成功)
	// 读旁路模式的操作
	if r.cache != nil && resultNode != nil { // 确保有缓存实例且节点非空
		setErr := r.cache.SetNode(ctx, id, resultNode, defaultNodeTTL) // 使用默认 TTL
		if setErr != nil {
			// 缓存设置失败通常不应阻塞主流程，记录警告即可
			fmt.Printf("WARN: Repo: 缓存设置节点失败 (id: %s): %v\n", id, setErr) // TODO: 使用日志库
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
		fmt.Printf("WARN: Repo: 更新后无法识别节点 (id: %s) 的标签: %v\n", req.ID, labels) // TODO: 使用日志库
		return nil, fmt.Errorf("repo: 更新后无法识别的节点类型标签 %v", labels)
	}
	updatedNode := mapDbNodeToThriftNode(dbNode, nodeType)

	// 4. 使缓存失效 (数据库操作成功后)
	if r.cache != nil { // 检查缓存是否已配置
		delErr := r.cache.DeleteNode(ctx, req.ID)
		if delErr != nil && !errors.Is(delErr, cache.ErrNotFound) { // 忽略 NotFound 错误
			// 删除缓存失败通常记录警告
			fmt.Printf("WARN: Repo: 缓存删除节点失败 (id: %s): %v\n", req.ID, delErr) // TODO: 使用日志库
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
			fmt.Printf("WARN: Repo: 缓存删除节点失败 (id: %s): %v\n", id, delErr) // TODO: 使用日志库
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
	// 使用 SHA1 哈希 Keyword 以避免过长或包含特殊字符的键
	hasher := sha1.New()
	hasher.Write([]byte(req.Keyword)) // Keyword 是 string 类型，可以直接写入
	keywordHash := hex.EncodeToString(hasher.Sum(nil))

	// 处理可能为 nil 的 Limit 和 Offset
	var limitVal, offsetVal int32
	if req.Limit != nil {
		limitVal = *req.Limit
	}
	if req.Offset != nil {
		offsetVal = *req.Offset
	}

	// NodeType 也是必需的
	nodeTypeStr := req.Type.String()

	// 格式: prefix:keyword_hash:type:limit:offset
	return fmt.Sprintf("%s%s:%s:%d:%d", searchNodesCachePrefix, keywordHash, nodeTypeStr, limitVal, offsetVal)
}

// SearchNodes 搜索节点 (带缓存)
func (r *neo4jNodeRepo) SearchNodes(ctx context.Context, req *network.SearchNodesRequest) ([]*network.Node, int32, error) {
	// 0. 检查缓存是否可用
	if r.cache == nil {
		fmt.Println("WARN: Repo: Cache 未初始化，跳过 SearchNodes 缓存") // TODO: 使用日志库
		return r.searchNodesDirect(ctx, req)
	}

	// 1. 生成缓存键
	cacheKey := generateSearchNodesCacheKey(req)

	// 2. 尝试从缓存获取
	cachedData, err := r.cache.Get(ctx, cacheKey)
	if err == nil { // 缓存命中
		// 2.0 检查是否是空结果标记
		if bytes.Equal(cachedData, []byte(searchEmptyPlaceholder)) {
			fmt.Printf("INFO: Repo: SearchNodes 缓存命中空标记 (Key: %s)\n", cacheKey) // TODO: 使用日志库
			return []*network.Node{}, 0, nil                                    // 返回空结果
		}

		// 2.1 尝试解析缓存的 ID 列表和总数
		var cachedValue searchNodesCacheValue
		if err := json.NewDecoder(bytes.NewReader(cachedData)).Decode(&cachedValue); err == nil {
			// 2.1 缓存命中且解析成功
			fmt.Printf("INFO: Repo: SearchNodes 缓存命中 (Key: %s)\n", cacheKey) // TODO: 使用日志库
			resultNodes := make([]*network.Node, 0, len(cachedValue.NodeIDs))
			for _, nodeID := range cachedValue.NodeIDs {
				// 2.2 使用 GetNode 获取节点详情 (GetNode 会处理自己的缓存)
				node, getNodeErr := r.GetNode(ctx, nodeID) // 注意：这里可能需要处理 GetNode 返回的错误
				if getNodeErr != nil {
					// 如果节点未找到（可能在缓存结果生成后被删除），可以选择跳过或记录日志
					if errors.Is(getNodeErr, cache.ErrNotFound) || isNotFoundError(getNodeErr) {
						fmt.Printf("WARN: Repo: SearchNodes 缓存命中，但 GetNode 未找到节点 %s (可能已被删除)\n", nodeID) // TODO: 使用日志库
						continue
					}
					// 其他 GetNode 错误，可能需要中断或返回错误
					fmt.Printf("ERROR: Repo: SearchNodes 缓存命中，但 GetNode 失败 (ID: %s): %v\n", nodeID, getNodeErr) // TODO: 使用日志库
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
		fmt.Printf("ERROR: Repo: SearchNodes 缓存数据解析失败 (Key: %s): %v\n", cacheKey, err) // TODO: 使用日志库
	} else if !errors.Is(err, cache.ErrNotFound) {
		// 非 "Not Found" 的缓存读取错误，记录日志但继续执行数据库查询
		fmt.Printf("ERROR: Repo: SearchNodes 缓存读取失败 (Key: %s): %v\n", cacheKey, err) // TODO: 使用日志库
	} else {
		// 缓存未命中 (cache.ErrNotFound)
		fmt.Printf("INFO: Repo: SearchNodes 缓存未命中 (Key: %s)\n", cacheKey) // TODO: 使用日志库
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
			// 设置缓存，忽略 Set 的错误 (只记录日志)
			// 使用通用的 Set
			setErr := r.cache.Set(ctx, cacheKey, buffer.Bytes(), searchNodesCacheTTL)
			if setErr != nil {
				fmt.Printf("ERROR: Repo: SearchNodes 缓存写入失败 (Key: %s): %v\n", cacheKey, setErr) // TODO: 使用日志库
			} else {
				fmt.Printf("INFO: Repo: SearchNodes 结果已写入缓存 (Key: %s)\n", cacheKey) // TODO: 使用日志库
			}
		} else {
			fmt.Printf("ERROR: Repo: SearchNodes 缓存值序列化失败 (Key: %s): %v\n", cacheKey, err) // TODO: 使用日志库
		}
	} else if err == nil && len(resultNodes) == 0 {
		// 4.1 缓存空结果标记
		setErr := r.cache.Set(ctx, cacheKey, []byte(searchEmptyPlaceholder), searchEmptyTTL)
		if setErr != nil {
			fmt.Printf("ERROR: Repo: SearchNodes 缓存空标记写入失败 (Key: %s): %v\n", cacheKey, setErr) // TODO: 使用日志库
		} else {
			fmt.Printf("INFO: Repo: SearchNodes 空结果已写入缓存标记 (Key: %s)\n", cacheKey) // TODO: 使用日志库
		}
	}

	// 5. 返回从数据库获取的结果
	return resultNodes, total, nil
}

// searchNodesDirect 是实际执行数据库查询的逻辑 (从原 SearchNodes 提取)
func (r *neo4jNodeRepo) searchNodesDirect(ctx context.Context, req *network.SearchNodesRequest) ([]*network.Node, int32, error) {
	session := r.driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
	defer session.Close(ctx)

	// 构建 DAL 查询条件
	criteria := make(map[string]string)
	if req.Keyword != "" { // Keyword 是 string 类型
		// 假设按 name 模糊搜索，DAL 层会处理 CONTAINS 逻辑
		criteria["name"] = req.Keyword
	}
	// 如果 SearchNodesRequest 中添加了 Properties 字段，则在此处处理
	// if req.Properties != nil { ... }

	// 处理分页参数
	var limit, offset int64
	if req.Limit != nil {
		limit = int64(*req.Limit)
	} else {
		limit = 10 // TODO:从config文件中读取limit
	}
	if req.Offset != nil {
		offset = int64(*req.Offset)
	} else {
		offset = 0 // 默认偏移
	}

	// 调用 DAL 层执行搜索
	dbNodes, labelsList, total, err := r.nodeDAL.ExecSearchNodes(ctx, session, criteria, req.Type, limit, offset)
	if err != nil {
		// 注意：这里不需要检查 isNotFoundError，因为搜索本身找不到是正常情况，DAL应返回空列表和0 total
		return nil, 0, fmt.Errorf("repo: 调用 DAL 搜索节点失败: %w", err)
	}

	// 映射结果
	resultNodes := make([]*network.Node, 0, len(dbNodes))
	for i, dbNode := range dbNodes {
		labels := labelsList[i]
		nodeType, ok := labelToNodeType(labels)
		if !ok {
			nodeID := getStringProp(dbNode.Props, "id", "[未知ID]")
			fmt.Printf("WARN: Repo: 搜索结果中无法识别节点 (id: %s) 的标签: %v\n", nodeID, labels) // TODO: 使用日志库
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
	// 包含 Profession (可能为空), maxDepth, limit, offset
	// 使用 Profession 值本身，如果可能很长或包含特殊字符，可以考虑哈希
	profession := req.Profession // Profession 是 string 类型
	return fmt.Sprintf("%s%s:%d:%d:%d", getNetworkCachePrefix, profession, maxDepth, limit, offset)
}

// GetNetwork 获取网络图谱 (节点和关系)，带缓存
// TODO: 从config文件中读取maxDepth
func (r *neo4jNodeRepo) GetNetwork(ctx context.Context, req *network.GetNetworkRequest) ([]*network.Node, []*network.Relation, error) {
	// 1. 处理参数和计算默认值 (与缓存键生成相关)
	var maxDepth int32 = 1 // 默认深度
	if req.IsSetDepth() {
		maxDepth = *req.Depth
		if maxDepth <= 0 || maxDepth > 5 {
			maxDepth = 1
		}
	}
	var limit int64 = 100 // 默认限制
	var offset int64 = 0  // 默认偏移

	// 2. 检查缓存和 RelationRepository 是否可用
	if r.cache == nil || r.relationRepo == nil {
		fmt.Println("WARN: Repo: GetNetwork cache or relationRepo not initialized, skipping cache.") // TODO: Use logger
		return r.getNetworkDirect(ctx, req, maxDepth, limit, offset)
	}

	// 3. 生成缓存键
	cacheKey := generateGetNetworkCacheKey(req, maxDepth, limit, offset)

	// 4. 尝试从缓存获取
	cachedData, err := r.cache.Get(ctx, cacheKey)
	if err == nil { // 缓存命中
		// 4.1 检查空标记
		if bytes.Equal(cachedData, []byte(getNetworkEmptyPlaceholder)) {
			fmt.Printf("INFO: Repo: GetNetwork cache hit empty placeholder (Key: %s)\n", cacheKey) // TODO: Use logger
			return []*network.Node{}, []*network.Relation{}, nil
		}

		// 4.2 解析缓存的 ID 列表
		var cachedValue getNetworkCacheValue
		if err := json.NewDecoder(bytes.NewReader(cachedData)).Decode(&cachedValue); err == nil {
			fmt.Printf("INFO: Repo: GetNetwork cache hit (Key: %s), fetching details...\n", cacheKey) // TODO: Use logger
			resultNodes := make([]*network.Node, 0, len(cachedValue.NodeIDs))
			resultRelations := make([]*network.Relation, 0, len(cachedValue.RelationIDs))
			failedFetches := 0

			// 4.3 使用 GetNode 获取节点
			for _, nodeID := range cachedValue.NodeIDs {
				node, getNodeErr := r.GetNode(ctx, nodeID)
				if getNodeErr != nil {
					failedFetches++
					if errors.Is(getNodeErr, cache.ErrNotFound) || isNotFoundError(getNodeErr) || errors.Is(getNodeErr, cache.ErrNilValue) {
						fmt.Printf("WARN: Repo: GetNetwork cache hit, but GetNode couldn't find node %s (may be deleted or nil cached)\n", nodeID) // TODO: Use logger
					} else {
						fmt.Printf("ERROR: Repo: GetNetwork cache hit, but GetNode failed for %s: %v\n", nodeID, getNodeErr) // TODO: Use logger
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
						fmt.Printf("WARN: Repo: GetNetwork cache hit, but GetRelation couldn't find relation %s (may be deleted or nil cached)\n", relationID) // TODO: Use logger
					} else {
						fmt.Printf("ERROR: Repo: GetNetwork cache hit, but GetRelation failed for %s: %v\n", relationID, getRelErr) // TODO: Use logger
					}
					continue // 跳过获取失败的关系
				}
				if relation != nil {
					resultRelations = append(resultRelations, relation)
				}
			}

			if failedFetches > 0 {
				fmt.Printf("WARN: Repo: GetNetwork cache hit, but %d nodes/relations failed to fetch from detail cache/DB (Key: %s)\n", failedFetches, cacheKey) // TODO: Use logger
			}
			return resultNodes, resultRelations, nil
		}
		// 缓存数据解析失败，当作未命中
		fmt.Printf("ERROR: Repo: GetNetwork cache data decode failed (Key: %s): %v\n", cacheKey, err) // TODO: Use logger

	} else if !errors.Is(err, cache.ErrNotFound) {
		// 非 NotFound 的缓存错误，记录并继续查库
		fmt.Printf("ERROR: Repo: GetNetwork cache get failed (Key: %s): %v\n", cacheKey, err) // TODO: Use logger
	} else {
		fmt.Printf("INFO: Repo: GetNetwork cache miss (Key: %s)\n", cacheKey) // TODO: Use logger
	}

	// 5. 缓存未命中或出错，直接查询数据库
	resultNodes, resultRelations, dbNodes, dbRelations, err := r.getNetworkDirectAndRaw(ctx, req, maxDepth, limit, offset)
	if err != nil {
		return nil, nil, err // 直接返回数据库错误
	}

	// 6. 缓存结果
	if err == nil {
		if len(dbNodes) == 0 && len(dbRelations) == 0 {
			// 6.1 缓存空标记
			setErr := r.cache.Set(ctx, cacheKey, []byte(getNetworkEmptyPlaceholder), getNetworkEmptyTTL)
			if setErr != nil {
				fmt.Printf("ERROR: Repo: GetNetwork cache set empty placeholder failed (Key: %s): %v\n", cacheKey, setErr) // TODO: Use logger
			} else {
				fmt.Printf("INFO: Repo: GetNetwork set empty placeholder to cache (Key: %s)\n", cacheKey) // TODO: Use logger
			}
		} else {
			// 6.2 提取 ID 并缓存
			nodeIDs := make([]string, 0, len(dbNodes))
			for _, dbNode := range dbNodes {
				if nodeID := getStringProp(dbNode.Props, "id", ""); nodeID != "" {
					nodeIDs = append(nodeIDs, nodeID)
				}
			}
			relationIDs := make([]string, 0, len(dbRelations))
			for _, dbRel := range dbRelations {
				if relID := getStringProp(dbRel.Props, "id", ""); relID != "" {
					relationIDs = append(relationIDs, relID)
				}
			}

			cacheValue := getNetworkCacheValue{
				NodeIDs:     nodeIDs,
				RelationIDs: relationIDs,
			}
			var buffer bytes.Buffer
			if err := json.NewEncoder(&buffer).Encode(cacheValue); err == nil {
				setErr := r.cache.Set(ctx, cacheKey, buffer.Bytes(), getNetworkCacheTTL)
				if setErr != nil {
					fmt.Printf("ERROR: Repo: GetNetwork cache set failed (Key: %s): %v\n", cacheKey, setErr) // TODO: Use logger
				} else {
					fmt.Printf("INFO: Repo: GetNetwork set data to cache (Key: %s)\n", cacheKey) // TODO: Use logger
				}
			} else {
				fmt.Printf("ERROR: Repo: GetNetwork cache value encode failed (Key: %s): %v\n", cacheKey, err) // TODO: Use logger
			}
		}
	}

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

	// 调用 DAL 层获取网络数据
	dbNodes, dbRelations, err = r.nodeDAL.ExecGetNetwork(ctx, session, req.Profession, maxDepth, limit, offset)
	if err != nil {
		err = fmt.Errorf("repo: 调用 DAL 获取网络失败: %w", err)
		return // 返回错误和空的 slices
	}

	// 映射节点
	nodesMap := make(map[int64]*network.Node) // 使用 Neo4j 内部 ID 暂存以处理关系
	resultNodes = make([]*network.Node, 0, len(dbNodes))
	for _, dbNode := range dbNodes {
		nodeType, ok := labelToNodeType(dbNode.Labels)
		if !ok {
			fmt.Printf("WARN: Repo: GetNetwork 中无法识别节点 (elementId: %s) 的标签: %v\n", dbNode.ElementId, dbNode.Labels) // TODO: logger
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
			fmt.Printf("WARN: Repo: GetNetwork 中关系 (elementId: %s) 的源或目标节点不在返回的节点列表中 (StartId: %d, EndId: %d)\n", dbRel.ElementId, dbRel.StartId, dbRel.EndId) // TODO: logger
			continue
		}

		relType, ok := stringToRelationType(dbRel.Type)
		if !ok {
			fmt.Printf("WARN: Repo: GetNetwork 中无法识别关系 (elementId: %s) 的类型: %s\n", dbRel.ElementId, dbRel.Type) // TODO: logger
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
	return fmt.Sprintf("%s%s:%s:%d:%s", getPathCachePrefix, req.SourceID, req.TargetID, maxDepth, typesHash)
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
		fmt.Println("WARN: Repo: GetPath cache or relationRepo not initialized, skipping cache.") // TODO: Use logger
		return r.getPathDirect(ctx, req, maxDepth, relationTypesStr)
	}

	// 3. 生成缓存键
	cacheKey := generateGetPathCacheKey(req, maxDepth, relationTypesStr)

	// 4. 尝试从缓存获取
	cachedData, err := r.cache.Get(ctx, cacheKey)
	if err == nil { // 缓存命中
		// 4.1 检查空标记
		if bytes.Equal(cachedData, []byte(getPathEmptyPlaceholder)) {
			fmt.Printf("INFO: Repo: GetPath cache hit empty placeholder (Key: %s)\n", cacheKey) // TODO: Use logger
			// 路径未找到，根据接口约定，可能需要返回特定错误而非空列表
			// return []*network.Node{}, []*network.Relation{}, nil // 或者返回原始的 not found 错误?
			return nil, nil, fmt.Errorf("repo: path not found (cached empty)") // 返回一个表示未找到的错误
		}

		// 4.2 解析缓存的 ID 列表
		var cachedValue getPathCacheValue
		if err := json.NewDecoder(bytes.NewReader(cachedData)).Decode(&cachedValue); err == nil {
			fmt.Printf("INFO: Repo: GetPath cache hit (Key: %s), fetching details...\n", cacheKey) // TODO: Use logger
			resultNodes := make([]*network.Node, 0, len(cachedValue.NodeIDs))
			resultRelations := make([]*network.Relation, 0, len(cachedValue.RelationIDs))
			failedFetches := 0

			// 4.3 按顺序获取节点
			for _, nodeID := range cachedValue.NodeIDs {
				node, getNodeErr := r.GetNode(ctx, nodeID)
				if getNodeErr != nil {
					failedFetches++
					if errors.Is(getNodeErr, cache.ErrNotFound) || isNotFoundError(getNodeErr) || errors.Is(getNodeErr, cache.ErrNilValue) {
						fmt.Printf("WARN: Repo: GetPath cache hit, but GetNode couldn't find node %s in path\n", nodeID) // TODO: Use logger
					} else {
						fmt.Printf("ERROR: Repo: GetPath cache hit, but GetNode failed for %s: %v\n", nodeID, getNodeErr) // TODO: Use logger
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
						fmt.Printf("WARN: Repo: GetPath cache hit, but GetRelation couldn't find relation %s in path\n", relationID) // TODO: Use logger
					} else {
						fmt.Printf("ERROR: Repo: GetPath cache hit, but GetRelation failed for %s: %v\n", relationID, getRelErr) // TODO: Use logger
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
		fmt.Printf("ERROR: Repo: GetPath cache data decode failed (Key: %s): %v\n", cacheKey, err) // TODO: Use logger
	} else if !errors.Is(err, cache.ErrNotFound) {
		// 非 NotFound 的缓存错误，记录并继续查库
		fmt.Printf("ERROR: Repo: GetPath cache get failed (Key: %s): %v\n", cacheKey, err) // TODO: Use logger
	} else {
		fmt.Printf("INFO: Repo: GetPath cache miss (Key: %s)\n", cacheKey) // TODO: Use logger
	}

	// 5. 缓存未命中或出错，直接查询数据库
	resultNodes, resultRelations, dbNodes, dbRelations, err := r.getPathDirectAndRaw(ctx, req, maxDepth, relationTypesStr)
	if err != nil {
		// 5.1 如果是路径未找到错误，缓存空标记
		if isNotFoundError(err) { // 检查是否是 DAL 返回的 Not Found
			setErr := r.cache.Set(ctx, cacheKey, []byte(getPathEmptyPlaceholder), getPathEmptyTTL)
			if setErr != nil {
				fmt.Printf("ERROR: Repo: GetPath cache set empty placeholder failed (Key: %s): %v\n", cacheKey, setErr) // TODO: Use logger
			} else {
				fmt.Printf("INFO: Repo: GetPath set empty placeholder to cache (Key: %s)\n", cacheKey) // TODO: Use logger
			}
			return nil, nil, err // 透传原始的 Not Found 错误
		}
		// 其他数据库错误
		return nil, nil, err
	}

	// 6. 缓存结果 (查询成功)
	if err == nil && (len(dbNodes) > 0 || len(dbRelations) > 0) { // 确保有路径数据才缓存
		// 6.1 按顺序提取 ID
		nodeIDs := make([]string, 0, len(dbNodes))
		for _, dbNode := range dbNodes {
			if nodeID := getStringProp(dbNode.Props, "id", ""); nodeID != "" {
				nodeIDs = append(nodeIDs, nodeID)
			} else {
				fmt.Printf("ERROR: Repo: GetPath DB result node missing 'id' property (ElementId: %s)\n", dbNode.ElementId) // TODO: logger
				// 如果节点没有ID，无法缓存，可能返回错误或跳过缓存
				goto SkipCache // 跳到缓存步骤之后
			}
		}
		relationIDs := make([]string, 0, len(dbRelations))
		for _, dbRel := range dbRelations {
			if relID := getStringProp(dbRel.Props, "id", ""); relID != "" {
				relationIDs = append(relationIDs, relID)
			} else {
				fmt.Printf("ERROR: Repo: GetPath DB result relation missing 'id' property (ElementId: %s)\n", dbRel.ElementId) // TODO: logger
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
			setErr := r.cache.Set(ctx, cacheKey, buffer.Bytes(), getPathCacheTTL)
			if setErr != nil {
				fmt.Printf("ERROR: Repo: GetPath cache set failed (Key: %s): %v\n", cacheKey, setErr) // TODO: Use logger
			} else {
				fmt.Printf("INFO: Repo: GetPath set data to cache (Key: %s)\n", cacheKey) // TODO: Use logger
			}
		} else {
			fmt.Printf("ERROR: Repo: GetPath cache value encode failed (Key: %s): %v\n", cacheKey, encErr) // TODO: Use logger
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
		if isNotFoundError(err) { // DAL 返回 Not Found 时，直接包装并返回
			err = fmt.Errorf("repo: path not found between %s and %s: %w", req.SourceID, req.TargetID, err)
			return
		}
		err = fmt.Errorf("repo: 调用 DAL 获取路径失败: %w", err)
		return // 返回其他 DAL 错误
	}

	// 映射节点 (保持顺序)
	nodesMap := make(map[int64]*network.Node) // 仍然需要 Map 来查找关系端点
	resultNodes = make([]*network.Node, len(dbNodes))
	for i, dbNode := range dbNodes {
		nodeType, ok := labelToNodeType(dbNode.Labels)
		if !ok {
			fmt.Printf("WARN: Repo: GetPath 中无法识别节点 (elementId: %s) 的标签: %v\n", dbNode.ElementId, dbNode.Labels) // TODO: logger
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
			fmt.Printf("ERROR: Repo: GetPath 中关系 (elementId: %s) 的源或目标节点查找失败 (StartId: %d, EndId: %d)\n", dbRel.ElementId, dbRel.StartId, dbRel.EndId) // TODO: logger
			err = fmt.Errorf("repo: 路径中关系端点查找失败")
			return // 映射失败
		}

		relType, ok := stringToRelationType(dbRel.Type)
		if !ok {
			fmt.Printf("WARN: Repo: GetPath 中无法识别关系 (elementId: %s) 的类型: %s\n", dbRel.ElementId, dbRel.Type) // TODO: logger
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
