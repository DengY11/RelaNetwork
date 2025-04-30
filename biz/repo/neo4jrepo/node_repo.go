package neo4jrepo

import (
	"context"
	"fmt" // Placeholder for error wrapping if needed
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/dbtype"

	network "labelwall/biz/model/relationship/network" // 确保路径正确
)

// NodeRepository 定义了节点数据访问的操作
type NodeRepository interface {
	CreateNode(ctx context.Context, req *network.CreateNodeRequest) (*network.Node, error)
	GetNode(ctx context.Context, id string) (*network.Node, error)
	UpdateNode(ctx context.Context, req *network.UpdateNodeRequest) (*network.Node, error)
	DeleteNode(ctx context.Context, id string) error
	SearchNodes(ctx context.Context, req *network.SearchNodesRequest) ([]*network.Node, int32, error)
	GetNetwork(ctx context.Context, req *network.GetNetworkRequest) ([]*network.Node, []*network.Relation, error)
	GetPath(ctx context.Context, req *network.GetPathRequest) ([]*network.Node, []*network.Relation, error)
}

// neo4jNodeRepo 实现了 NodeRepository 接口
type neo4jNodeRepo struct {
	driver neo4j.DriverWithContext
}

// NewNodeRepository 创建一个新的 NodeRepository 实例
func NewNodeRepository(driver neo4j.DriverWithContext) NodeRepository {
	// 可以在这里添加必要的索引和约束创建逻辑，确保它们在服务启动时存在
	// 例如: initNodeSchema(driver)
	return &neo4jNodeRepo{driver: driver}
}

// CreateNode 在 Neo4j 中创建一个新节点
func (r *neo4jNodeRepo) CreateNode(ctx context.Context, req *network.CreateNodeRequest) (*network.Node, error) {
	session := r.driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)

	// 使用UUID或其他方式生成唯一节点ID
	// 这里暂时使用时间戳作为示例，生产环境应使用更可靠的唯一ID生成策略
	nodeID := fmt.Sprintf("%s-%d", req.Type.String(), time.Now().UnixNano())

	nodeResult, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		// 构建节点属性Map
		properties := map[string]any{
			"id":   nodeID, // 使用生成的业务ID
			"name": req.Name,
			// Neo4j驱动通常会自动处理 nil 值，无需显式检查 optional 字段是否为 nil
			"avatar":     req.Avatar,
			"profession": req.Profession,
		}
		// 合并可选的 properties
		if req.Properties != nil {
			for k, v := range req.Properties {
				properties[k] = v // 注意：确保 key 不会覆盖 id, name 等核心属性
			}
		}

		// 构建 Cypher 查询
		// 使用 $param 语法进行参数化，防止注入
		// 使用 NodeType 的字符串表示作为标签
		query := fmt.Sprintf(`
            CREATE (n:%s $props)
            RETURN n`, req.Type.String()) // NodeType -> Label (e.g., PERSON)

		result, err := tx.Run(ctx, query, map[string]any{"props": properties})
		if err != nil {
			return nil, fmt.Errorf("运行创建节点查询失败: %w", err)
		}

		record, err := result.Single(ctx)
		if err != nil {
			return nil, fmt.Errorf("获取创建节点结果失败: %w", err)
		}

		nodeInterface, ok := record.Get("n")
		if !ok {
			return nil, fmt.Errorf("无法从结果中获取节点 'n'")
		}

		dbNode, ok := nodeInterface.(dbtype.Node)
		if !ok {
			return nil, fmt.Errorf("结果中的 'n' 不是有效的节点类型")
		}

		return mapDbNodeToThriftNode(dbNode, req.Type), nil // 将 Neo4j 节点映射回 Thrift 节点结构
	})

	if err != nil {
		return nil, err // 错误已在 ExecuteWrite 中封装
	}

	createdNode, ok := nodeResult.(*network.Node)
	if !ok {
		return nil, fmt.Errorf("事务返回了非预期的节点类型")
	}

	return createdNode, nil
}

// mapDbNodeToThriftNode 将 Neo4j 节点对象转换为 Thrift Node 对象
// 需要传入 NodeType，因为 Neo4j 节点可能有多个标签，而 Thrift 模型只有一个 type 字段
func mapDbNodeToThriftNode(dbNode dbtype.Node, nodeType network.NodeType) *network.Node {
	props := dbNode.Props
	node := &network.Node{
		Type: nodeType, // 使用传入的类型
		// 从属性中提取核心字段
		ID:         getStringProp(props, "id", ""),   // 修复：使用大写的 ID
		Name:       getStringProp(props, "name", ""), // 假设 name 总是存在
		Avatar:     getOptionalStringProp(props, "avatar"),
		Profession: getOptionalStringProp(props, "profession"),
		Properties: make(map[string]string), // 初始化 map
	}

	// 填充其他属性到 Properties map
	coreProps := map[string]struct{}{"id": {}, "name": {}, "avatar": {}, "profession": {}} // 这里的 "id" 是 Neo4j 属性名，保持小写
	for key, val := range props {
		if _, isCore := coreProps[key]; !isCore {
			if strVal, ok := val.(string); ok { // 假设所有额外属性都是 string
				node.Properties[key] = strVal
			}
			// 可以根据需要添加对其他类型的处理
		}
	}
	if len(node.Properties) == 0 {
		node.Properties = nil // 如果没有额外属性，设为 nil
	}

	return node
}

// --- 辅助函数 ---

func getStringProp(props map[string]any, key string, defaultValue string) string {
	if val, ok := props[key].(string); ok {
		return val
	}
	return defaultValue
}

func getOptionalStringProp(props map[string]any, key string) *string {
	if val, ok := props[key].(string); ok {
		// Thrift optional string 是指针类型
		return &val
	}
	return nil
}

// --- 其他方法的占位实现 ---

func (r *neo4jNodeRepo) GetNode(ctx context.Context, id string) (*network.Node, error) {
	// TODO: 实现 GetNode 查询
	fmt.Printf("TODO: 实现 GetNode 查询, id: %s\n", id)
	return nil, fmt.Errorf("GetNode 未实现")
}

func (r *neo4jNodeRepo) UpdateNode(ctx context.Context, req *network.UpdateNodeRequest) (*network.Node, error) {
	// TODO: 实现 UpdateNode 查询
	fmt.Printf("TODO: 实现 UpdateNode 查询, id: %s\n", req.ID) // 修复：使用大写的 ID
	return nil, fmt.Errorf("UpdateNode 未实现")
}

func (r *neo4jNodeRepo) DeleteNode(ctx context.Context, id string) error {
	// TODO: 实现 DeleteNode 查询
	fmt.Printf("TODO: 实现 DeleteNode 查询, id: %s\n", id)
	return fmt.Errorf("DeleteNode 未实现")
}

func (r *neo4jNodeRepo) SearchNodes(ctx context.Context, req *network.SearchNodesRequest) ([]*network.Node, int32, error) {
	// TODO: 实现 SearchNodes 查询
	fmt.Printf("TODO: 实现 SearchNodes 查询, keyword: %s\n", req.Keyword)
	return nil, 0, fmt.Errorf("SearchNodes 未实现")
}

func (r *neo4jNodeRepo) GetNetwork(ctx context.Context, req *network.GetNetworkRequest) ([]*network.Node, []*network.Relation, error) {
	// TODO: 实现 GetNetwork 查询
	fmt.Printf("TODO: 实现 GetNetwork 查询, profession: %s\n", req.Profession)
	return nil, nil, fmt.Errorf("GetNetwork 未实现")
}

func (r *neo4jNodeRepo) GetPath(ctx context.Context, req *network.GetPathRequest) ([]*network.Node, []*network.Relation, error) {
	// TODO: 实现 GetPath 查询
	fmt.Printf("TODO: 实现 GetPath 查询, source: %s, target: %s\n", req.SourceID, req.TargetID) // 修复：使用大写的 SourceID, TargetID
	return nil, nil, fmt.Errorf("GetPath 未实现")
}

// 可选：添加一个初始化函数来创建索引和约束
// func initNodeSchema(driver neo4j.DriverWithContext) {
// 	ctx := context.Background()
// 	session := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
// 	defer session.Close(ctx)
//
// 	indexes := []string{
// 		"CREATE CONSTRAINT unique_person_id IF NOT EXISTS FOR (p:PERSON) REQUIRE p.id IS UNIQUE;",
// 		"CREATE INDEX index_person_id IF NOT EXISTS FOR (p:PERSON) ON (p.id);",
//      "CREATE INDEX index_person_profession IF NOT EXISTS FOR (p:PERSON) ON (p.profession);",
// 		// 为 COMPANY, SCHOOL 添加类似约束和索引...
// 	}
//
// 	for _, query := range indexes {
// 		_, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
// 			_, err := tx.Run(ctx, query, nil)
// 			return nil, err
// 		})
// 		if err != nil {
// 			// 在实际应用中应使用日志库记录错误
// 			fmt.Printf("应用 schema 失败 (%s): %v\n", query, err)
// 		}
// 	}
// }
