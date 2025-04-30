package neo4jrepo

import (
	"context"
	"fmt" // Placeholder for error wrapping if needed
	"time"

	"github.com/google/uuid" // 推荐使用 UUID 生成 ID
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/dbtype"

	"labelwall/biz/dal/neo4jdal"                       // 导入 DAL 包
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
	driver  neo4j.DriverWithContext
	nodeDAL neo4jdal.NodeDAL
}

// NewNodeRepository 创建一个新的 NodeRepository 实例
func NewNodeRepository(driver neo4j.DriverWithContext, nodeDAL neo4jdal.NodeDAL) NodeRepository {
	// 约束和索引的创建仍然建议放在 infrastructure 或 main 初始化中
	return &neo4jNodeRepo{
		driver:  driver,
		nodeDAL: nodeDAL,
	}
}

// CreateNode 在 Neo4j 中创建一个新节点
func (r *neo4jNodeRepo) CreateNode(ctx context.Context, req *network.CreateNodeRequest) (*network.Node, error) {
	session := r.driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)

	// 1. 生成唯一业务 ID (Repo 层职责)
	// nodeID := fmt.Sprintf("%s-%d", req.Type.String(), time.Now().UnixNano()) // 旧方法
	nodeID := uuid.NewString() // 使用 UUID

	// 2. 构建节点属性 Map (Repo 层职责)
	properties := map[string]any{
		"id":   nodeID,
		"name": req.Name,
		// 可选字段处理
		"avatar":     req.Avatar,
		"profession": req.Profession,
		// 添加时间戳等通用字段
		"created_at": time.Now().UTC(),
		"updated_at": time.Now().UTC(),
	}
	if req.Properties != nil {
		for k, v := range req.Properties {
			if _, exists := properties[k]; !exists { // 避免覆盖核心属性
				properties[k] = v
			}
		}
	}

	// 3. 调用 DAL 层执行数据库操作
	// 注意：这里直接在 session 上调用 DAL 方法。如果需要跨多个 DAL 操作的事务，
	// 则应在此处开始事务(session.BeginTransaction)，并将事务对象(tx)传递给 DAL 方法。
	// 为简单起见，这里假设每个操作都在自己的隐式事务中（通过 session 调用 DAL 的 ExecuteWrite）。
	dbNode, err := r.nodeDAL.ExecCreateNode(ctx, session, req.Type, properties)
	if err != nil {
		return nil, fmt.Errorf("Repo: 调用 DAL 创建节点失败: %w", err)
	}

	// 4. 将 DAL 返回的 dbtype.Node 映射为业务模型 network.Node (Repo 层职责)
	return mapDbNodeToThriftNode(dbNode, req.Type), nil
}

// mapDbNodeToThriftNode 将 Neo4j 节点对象转换为 Thrift Node 对象
// (这个函数保留在 Repo 层，因为它处理的是业务模型映射)
func mapDbNodeToThriftNode(dbNode dbtype.Node, nodeType network.NodeType) *network.Node {
	props := dbNode.Props
	node := &network.Node{
		Type:       nodeType,
		ID:         getStringProp(props, "id", ""),
		Name:       getStringProp(props, "name", ""),
		Avatar:     getOptionalStringProp(props, "avatar"),
		Profession: getOptionalStringProp(props, "profession"),
		Properties: make(map[string]string),
	}

	coreProps := map[string]struct{}{ // 包含所有核心和通用字段
		"id": {}, "name": {}, "avatar": {}, "profession": {}, "created_at": {}, "updated_at": {},
	}
	for key, val := range props {
		if _, isCore := coreProps[key]; !isCore {
			if strVal, ok := val.(string); ok {
				node.Properties[key] = strVal
			} // 可以添加对其他类型的处理，例如 time.Time -> string
		}
	}
	if len(node.Properties) == 0 {
		node.Properties = nil
	}

	return node
}

// --- 辅助函数 (保留在 Repo 层或移至公共 pkg) ---

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

// --- 其他方法的占位实现 (需要重构以调用 DAL) ---

func (r *neo4jNodeRepo) GetNode(ctx context.Context, id string) (*network.Node, error) {
	// TODO: 重构 GetNode
	// 1. 获取 Session
	// 2. 调用 r.nodeDAL.ExecGetNodeByID(ctx, session, id)
	// 3. 处理错误
	// 4. 调用 mapDbNodeToThriftNode 映射结果
	fmt.Printf("TODO: Repo 实现 GetNode, id: %s\n", id)
	return nil, fmt.Errorf("Repo: GetNode 未实现")
}

func (r *neo4jNodeRepo) UpdateNode(ctx context.Context, req *network.UpdateNodeRequest) (*network.Node, error) {
	// TODO: 重构 UpdateNode
	fmt.Printf("TODO: Repo 实现 UpdateNode, id: %s\n", req.ID) // 修复：使用大写的 ID
	return nil, fmt.Errorf("Repo: UpdateNode 未实现")
}

func (r *neo4jNodeRepo) DeleteNode(ctx context.Context, id string) error {
	// TODO: 重构 DeleteNode
	fmt.Printf("TODO: Repo 实现 DeleteNode, id: %s\n", id)
	return fmt.Errorf("Repo: DeleteNode 未实现")
}

func (r *neo4jNodeRepo) SearchNodes(ctx context.Context, req *network.SearchNodesRequest) ([]*network.Node, int32, error) {
	// TODO: 重构 SearchNodes
	fmt.Printf("TODO: Repo 实现 SearchNodes, keyword: %s\n", req.Keyword)
	return nil, 0, fmt.Errorf("Repo: SearchNodes 未实现")
}

func (r *neo4jNodeRepo) GetNetwork(ctx context.Context, req *network.GetNetworkRequest) ([]*network.Node, []*network.Relation, error) {
	// TODO: 重构 GetNetwork
	fmt.Printf("TODO: Repo 实现 GetNetwork, profession: %s\n", req.Profession)
	return nil, nil, fmt.Errorf("Repo: GetNetwork 未实现")
}

func (r *neo4jNodeRepo) GetPath(ctx context.Context, req *network.GetPathRequest) ([]*network.Node, []*network.Relation, error) {
	// TODO: 重构 GetPath
	fmt.Printf("TODO: Repo 实现 GetPath, source: %s, target: %s\n", req.SourceID, req.TargetID) // 修复：使用大写的 SourceID, TargetID
	return nil, nil, fmt.Errorf("Repo: GetPath 未实现")
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
