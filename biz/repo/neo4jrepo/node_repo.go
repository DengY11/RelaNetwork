package neo4jrepo

import (
	"context" // 用于错误处理
	"fmt"     // 用于错误检查
	"time"

	"github.com/google/uuid"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"

	"labelwall/biz/dal/neo4jdal"
	network "labelwall/biz/model/relationship/network"
	//TODO "labelwall/pkg/cache" // 缓存包（待实现）
)

// neo4jNodeRepo 实现了 NodeRepository 接口
type neo4jNodeRepo struct {
	driver  neo4j.DriverWithContext
	nodeDAL neo4jdal.NodeDAL
	// cache   cache.NodeCache // 节点缓存接口（待实现）
}

// NewNodeRepository 创建一个新的 NodeRepository 实例
// 依赖注入 Neo4j 驱动、Node DAL 实现和可选的缓存实现
func NewNodeRepository(driver neo4j.DriverWithContext, nodeDAL neo4jdal.NodeDAL /*, cache cache.NodeCache*/) NodeRepository {
	return &neo4jNodeRepo{
		driver:  driver,
		nodeDAL: nodeDAL,
		//TODO: cache:   cache,
	}
}

// CreateNode 在 Neo4j 中创建一个新节点
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

// GetNode 通过 ID 获取节点
func (r *neo4jNodeRepo) GetNode(ctx context.Context, id string) (*network.Node, error) {
	/*
		// 1. 尝试从缓存获取
		if r.cache != nil {
			cachedNode, err := r.cache.Get(ctx, id)
			if err == nil && cachedNode != nil {
				fmt.Printf("Repo: GetNode cache hit for id: %s\n", id)
				return cachedNode, nil
			}
			if err != nil && !errors.Is(err, cache.ErrNotFound) {
				// 记录缓存错误，但继续尝试从数据库获取
				fmt.Printf("WARN: Repo: 缓存获取节点失败 (id: %s): %v\n", id, err)
			}
		}
	*/

	// 2. 从数据库获取
	session := r.driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
	defer session.Close(ctx)

	dbNode, labels, err := r.nodeDAL.ExecGetNodeByID(ctx, session, id)
	if err != nil {
		// 处理 DAL 返回的特定错误，例如未找到
		if isNotFoundError(err) { // 使用辅助函数检查错误
			return nil, err // 直接透传 Not Found 错误
		}
		return nil, fmt.Errorf("repo: 调用 DAL 获取节点失败: %w", err)
	}

	// 3. 将 DAL 返回的 dbtype.Node 映射为业务模型 network.Node
	nodeType, ok := labelToNodeType(labels) // 使用辅助函数推断类型
	if !ok {
		// 如果无法从标签推断出已知的 NodeType，记录警告或返回错误
		fmt.Printf("WARN: Repo: 无法识别节点 (id: %s) 的标签: %v\n", id, labels)
		// 决定如何处理：返回错误、使用默认值、或尝试其他逻辑
		return nil, fmt.Errorf("repo: 无法识别的节点类型标签 %v", labels)
	}
	resultNode := mapDbNodeToThriftNode(dbNode, nodeType) // 使用辅助函数映射

	/*
		// 4. 存入缓存 (如果需要且获取成功)
		if r.cache != nil && resultNode != nil {
			setErr := r.cache.Set(ctx, id, resultNode, time.Hour) // 假设缓存1小时
			if setErr != nil {
				fmt.Printf("WARN: Repo: 缓存设置节点失败 (id: %s): %v\n", id, setErr)
			}
		}
	*/

	return resultNode, nil
}

// UpdateNode 更新节点属性
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
		if isNotFoundError(err) { // 使用辅助函数检查错误
			return nil, err // 透传 Not Found
		}
		return nil, fmt.Errorf("repo: 调用 DAL 更新节点失败: %w", err)
	}

	// 3. 映射结果
	nodeType, ok := labelToNodeType(labels)
	if !ok {
		fmt.Printf("WARN: Repo: 更新后无法识别节点 (id: %s) 的标签: %v\n", req.ID, labels)
		return nil, fmt.Errorf("repo: 更新后无法识别的节点类型标签 %v", labels)
	}
	updatedNode := mapDbNodeToThriftNode(dbNode, nodeType)

	/*
		// 4. 使缓存失效
		if r.cache != nil {
			delErr := r.cache.Delete(ctx, req.ID)
			if delErr != nil && !errors.Is(delErr, cache.ErrNotFound) {
				fmt.Printf("WARN: Repo: 缓存删除节点失败 (id: %s): %v\n", req.ID, delErr)
			}
		}
	*/

	return updatedNode, nil
}

// DeleteNode 删除节点
func (r *neo4jNodeRepo) DeleteNode(ctx context.Context, id string) error {
	session := r.driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)

	// 1. 调用 DAL 层执行删除
	err := r.nodeDAL.ExecDeleteNode(ctx, session, id)
	if err != nil {
		if isNotFoundError(err) { // 使用辅助函数检查错误
			return err // 透传 Not Found
		}
		return fmt.Errorf("repo: 调用 DAL 删除节点失败: %w", err)
	}

	/*
		// 2. 使缓存失效
		if r.cache != nil {
			delErr := r.cache.Delete(ctx, id)
			if delErr != nil && !errors.Is(delErr, cache.ErrNotFound) {
				fmt.Printf("WARN: Repo: 缓存删除节点失败 (id: %s): %v\n", id, delErr)
			}
		}
	*/

	return nil
}

// SearchNodes 搜索节点
func (r *neo4jNodeRepo) SearchNodes(ctx context.Context, req *network.SearchNodesRequest) ([]*network.Node, int32, error) {
	session := r.driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
	defer session.Close(ctx)

	// 1. 构建 DAL 查询条件
	criteria := make(map[string]string)
	if req.Keyword != "" { // Keyword 是 string 类型
		// 假设按 name 模糊搜索，DAL 层会处理 CONTAINS 逻辑
		criteria["name"] = req.Keyword
	}
	// 如果 SearchNodesRequest 中添加了 Properties 字段，则在此处处理
	// if req.Properties != nil { ... }

	// 处理分页参数，需要从 *int32 转换为 int64
	var limit, offset int64
	if req.Limit != nil {
		limit = int64(*req.Limit)
	} else {
		limit = 10 // 默认分页大小
	}
	if req.Offset != nil {
		offset = int64(*req.Offset)
	} else {
		offset = 0 // 默认偏移
	}

	// 2. 调用 DAL 层执行搜索
	dbNodes, labelsList, total, err := r.nodeDAL.ExecSearchNodes(ctx, session, criteria, req.Type, limit, offset)
	if err != nil {
		return nil, 0, fmt.Errorf("repo: 调用 DAL 搜索节点失败: %w", err)
	}

	// 3. 映射结果
	resultNodes := make([]*network.Node, 0, len(dbNodes))
	for i, dbNode := range dbNodes {
		labels := labelsList[i]
		nodeType, ok := labelToNodeType(labels)
		if !ok {
			nodeID := getStringProp(dbNode.Props, "id", "[未知ID]")
			fmt.Printf("WARN: Repo: 搜索结果中无法识别节点 (id: %s) 的标签: %v\n", nodeID, labels)
			continue // 跳过无法识别类型的节点
		}
		thriftNode := mapDbNodeToThriftNode(dbNode, nodeType)
		if thriftNode != nil {
			resultNodes = append(resultNodes, thriftNode)
		}
	}

	return resultNodes, int32(total), nil
}

// GetNetwork 获取网络图谱 (节点和关系)
func (r *neo4jNodeRepo) GetNetwork(ctx context.Context, req *network.GetNetworkRequest) ([]*network.Node, []*network.Relation, error) {
	session := r.driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
	defer session.Close(ctx)

	// 1. 处理可选参数的默认值和解引用
	var maxDepth int32 = 1 // 默认深度为 1
	if req.IsSetDepth() {  // 使用 IsSetDepth 检查可选字段
		maxDepth = *req.Depth
		if maxDepth <= 0 || maxDepth > 5 { // 限制深度防止查询失控
			maxDepth = 1 // 或者返回错误，取决于业务需求
		}
	}

	// GetNetworkRequest 中没有 Limit/Offset, 在 Repo 层或 Service 层设定默认值
	var limit int64 = 100 // 默认限制
	var offset int64 = 0  // 默认偏移
	// 如果需要在请求中控制分页，需要在 Thrift 定义和 Service/Repo 层添加 Limit/Offset 字段

	// 2. 调用 DAL 层获取网络数据
	dbNodes, dbRelations, err := r.nodeDAL.ExecGetNetwork(ctx, session, req.Profession, maxDepth, limit, offset)
	if err != nil {
		return nil, nil, fmt.Errorf("repo: 调用 DAL 获取网络失败: %w", err)
	}

	// 3. 映射节点
	nodesMap := make(map[int64]*network.Node) // 使用 Neo4j 内部 ID 暂存以处理关系
	resultNodes := make([]*network.Node, 0, len(dbNodes))
	for _, dbNode := range dbNodes {
		nodeType, ok := labelToNodeType(dbNode.Labels)
		if !ok {
			fmt.Printf("WARN: Repo: GetNetwork 中无法识别节点 (elementId: %s) 的标签: %v\n", dbNode.ElementId, dbNode.Labels)
			continue // 跳过无法识别的节点
		}
		thriftNode := mapDbNodeToThriftNode(dbNode, nodeType)
		if thriftNode != nil {
			resultNodes = append(resultNodes, thriftNode)
			// 注意：这里使用 Neo4j 内部 ID 作为 key，因为关系数据使用内部 ID 连接
			nodesMap[dbNode.Id] = thriftNode
		}
	}

	// 4. 映射关系
	resultRelations := make([]*network.Relation, 0, len(dbRelations))
	for _, dbRel := range dbRelations {
		// 使用 Neo4j 内部 ID 查找业务 ID
		sourceNode, sourceExists := nodesMap[dbRel.StartId]
		targetNode, targetExists := nodesMap[dbRel.EndId]
		if !sourceExists || !targetExists {
			fmt.Printf("WARN: Repo: GetNetwork 中关系 (elementId: %s) 的源或目标节点不在返回的节点列表中 (StartId: %d, EndId: %d)\n", dbRel.ElementId, dbRel.StartId, dbRel.EndId)
			continue // 跳过悬空关系
		}

		relType, ok := stringToRelationType(dbRel.Type) // 从类型字符串推断枚举
		if !ok {
			fmt.Printf("WARN: Repo: GetNetwork 中无法识别关系 (elementId: %s) 的类型: %s\n", dbRel.ElementId, dbRel.Type)
			continue // 跳过无法识别类型的关系
		}

		// 使用业务 ID (sourceNode.ID, targetNode.ID) 创建 Thrift Relation
		thriftRelation := mapDbRelationshipToThriftRelation(dbRel, relType, sourceNode.ID, targetNode.ID)
		if thriftRelation != nil {
			resultRelations = append(resultRelations, thriftRelation)
		}
	}

	// TODO: 考虑是否缓存 GetNetwork 的结果

	return resultNodes, resultRelations, nil
}

// GetPath 获取两个节点之间的最短路径
func (r *neo4jNodeRepo) GetPath(ctx context.Context, req *network.GetPathRequest) ([]*network.Node, []*network.Relation, error) {
	session := r.driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
	defer session.Close(ctx)

	// 1. 处理可选参数
	var maxDepth int32 = 3   // 默认路径深度为 3
	if req.IsSetMaxDepth() { // 使用 IsSetMaxDepth 检查
		maxDepth = *req.MaxDepth
		if maxDepth <= 0 || maxDepth > 10 { // 限制深度
			maxDepth = 3 // 或返回错误
		}
	}

	// RelationTypes 是 []RelationType 类型，需要转换为 DAL 期望的 []string
	var relationTypesStr []string
	if req.IsSetTypes() && len(req.Types) > 0 { // 检查是否设置且非空
		relationTypesStr = make([]string, 0, len(req.Types))
		for _, rt := range req.Types {
			relationTypesStr = append(relationTypesStr, rt.String()) // 使用枚举的 String() 方法
		}
	}

	// 2. 调用 DAL 层获取路径数据
	dbNodes, dbRelations, err := r.nodeDAL.ExecGetPath(ctx, session, req.SourceID, req.TargetID, maxDepth, relationTypesStr)
	if err != nil {
		if isNotFoundError(err) { // 路径未找到也可能报 not found
			return nil, nil, err // 透传路径未找到
		}
		return nil, nil, fmt.Errorf("repo: 调用 DAL 获取路径失败: %w", err)
	}

	// 3. 映射节点 (路径中的节点顺序是重要的)
	nodesMap := make(map[int64]*network.Node)          // 仍然需要 Map 来查找关系端点
	resultNodes := make([]*network.Node, len(dbNodes)) // 预分配空间
	for i, dbNode := range dbNodes {
		nodeType, ok := labelToNodeType(dbNode.Labels)
		if !ok {
			fmt.Printf("WARN: Repo: GetPath 中无法识别节点 (elementId: %s) 的标签: %v\n", dbNode.ElementId, dbNode.Labels)
			return nil, nil, fmt.Errorf("repo: 路径中节点类型未知 (%v)", dbNode.Labels)
		}
		thriftNode := mapDbNodeToThriftNode(dbNode, nodeType)
		if thriftNode == nil {
			return nil, nil, fmt.Errorf("repo: 路径节点映射失败 (elementId: %s)", dbNode.ElementId)
		}
		resultNodes[i] = thriftNode
		nodesMap[dbNode.Id] = thriftNode
	}

	// 4. 映射关系 (路径中的关系顺序也是重要的)
	resultRelations := make([]*network.Relation, len(dbRelations)) // 预分配空间
	for i, dbRel := range dbRelations {
		sourceNode, sourceExists := nodesMap[dbRel.StartId]
		targetNode, targetExists := nodesMap[dbRel.EndId]
		if !sourceExists || !targetExists {
			fmt.Printf("ERROR: Repo: GetPath 中关系 (elementId: %s) 的源或目标节点查找失败 (StartId: %d, EndId: %d)\n", dbRel.ElementId, dbRel.StartId, dbRel.EndId)
			return nil, nil, fmt.Errorf("repo: 路径中关系端点查找失败")
		}

		relType, ok := stringToRelationType(dbRel.Type)
		if !ok {
			fmt.Printf("WARN: Repo: GetPath 中无法识别关系 (elementId: %s) 的类型: %s\n", dbRel.ElementId, dbRel.Type)
			return nil, nil, fmt.Errorf("repo: 路径中关系类型未知 (%s)", dbRel.Type)
		}

		thriftRelation := mapDbRelationshipToThriftRelation(dbRel, relType, sourceNode.ID, targetNode.ID)
		if thriftRelation == nil {
			return nil, nil, fmt.Errorf("repo: 路径关系映射失败 (elementId: %s)", dbRel.ElementId)
		}
		resultRelations[i] = thriftRelation
	}

	return resultNodes, resultRelations, nil
}
