package neo4jrepo

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid" // 推荐使用 UUID 生成关系 ID
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/dbtype"

	"labelwall/biz/dal/neo4jdal" // 导入 DAL 包
	network "labelwall/biz/model/relationship/network"
)

// RelationRepository 定义了关系数据访问的操作
type RelationRepository interface {
	CreateRelation(ctx context.Context, req *network.CreateRelationRequest) (*network.Relation, error)
	GetRelation(ctx context.Context, id string) (*network.Relation, error)
	UpdateRelation(ctx context.Context, req *network.UpdateRelationRequest) (*network.Relation, error)
	DeleteRelation(ctx context.Context, id string) error
	GetNodeRelations(ctx context.Context, req *network.GetNodeRelationsRequest) ([]*network.Relation, int32, error)
}

// neo4jRelationRepo 实现了 RelationRepository 接口
type neo4jRelationRepo struct {
	driver      neo4j.DriverWithContext // Repo 层持有 Driver 以管理 Session/Transaction
	relationDAL neo4jdal.RelationDAL    // 依赖 DAL 接口
}

// NewRelationRepository 创建一个新的 RelationRepository 实例
func NewRelationRepository(driver neo4j.DriverWithContext, relationDAL neo4jdal.RelationDAL) RelationRepository {
	return &neo4jRelationRepo{
		driver:      driver,
		relationDAL: relationDAL,
	}
}

// CreateRelation 在 Neo4j 中创建两个节点之间的新关系
func (r *neo4jRelationRepo) CreateRelation(ctx context.Context, req *network.CreateRelationRequest) (*network.Relation, error) {
	session := r.driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)

	// 1. 生成唯一关系 ID (Repo 层职责)
	// relationID := fmt.Sprintf("%s-%s-%d", req.Source, req.Target, time.Now().UnixNano()) // 旧方法
	relationID := uuid.NewString() // 使用 UUID

	// 2. 构建关系属性 Map (Repo 层职责)
	properties := map[string]any{
		"id":    relationID,
		"label": req.Label,
		// 添加时间戳
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
	dbRel, err := r.relationDAL.ExecCreateRelation(ctx, session, req.Source, req.Target, req.Type, properties)
	if err != nil {
		return nil, fmt.Errorf("Repo: 调用 DAL 创建关系失败: %w", err)
	}

	// 4. 将 DAL 返回的 dbtype.Relationship 映射为业务模型 network.Relation (Repo 层职责)
	return mapDbRelationshipToThriftRelation(dbRel, req.Type, req.Source, req.Target), nil
}

// mapDbRelationshipToThriftRelation 将 Neo4j 关系对象转换为 Thrift Relation 对象
// (这个函数保留在 Repo 层)
func mapDbRelationshipToThriftRelation(dbRel dbtype.Relationship, relType network.RelationType, sourceID, targetID string) *network.Relation {
	props := dbRel.Props
	relation := &network.Relation{
		Type:       relType,
		Source:     sourceID,
		Target:     targetID,
		ID:         getStringProp(props, "id", ""),
		Label:      getOptionalStringProp(props, "label"),
		Properties: make(map[string]string),
	}

	coreProps := map[string]struct{}{ // 包含所有核心和通用字段
		"id": {}, "label": {}, "created_at": {}, "updated_at": {},
	}
	for key, val := range props {
		if _, isCore := coreProps[key]; !isCore {
			if strVal, ok := val.(string); ok {
				relation.Properties[key] = strVal
			}
		}
	}
	if len(relation.Properties) == 0 {
		relation.Properties = nil
	}

	return relation
}

// --- 辅助函数 (应移至公共 pkg 或从 node_repo 导入) ---
// ... getStringProp, getOptionalStringProp ...

// --- 其他方法的占位实现 (需要重构以调用 DAL) ---

func (r *neo4jRelationRepo) GetRelation(ctx context.Context, id string) (*network.Relation, error) {
	// TODO: 重构 GetRelation
	// 1. 获取 Session
	// 2. 调用 r.relationDAL.ExecGetRelationByID(ctx, session, id)
	// 3. 处理错误
	// 4. 调用 mapDbRelationshipToThriftRelation 映射结果
	fmt.Printf("TODO: Repo 实现 GetRelation, id: %s\n", id)
	return nil, fmt.Errorf("Repo: GetRelation 未实现")
}

func (r *neo4jRelationRepo) UpdateRelation(ctx context.Context, req *network.UpdateRelationRequest) (*network.Relation, error) {
	// TODO: 重构 UpdateRelation
	fmt.Printf("TODO: Repo 实现 UpdateRelation, id: %s\n", req.ID)
	return nil, fmt.Errorf("Repo: UpdateRelation 未实现")
}

func (r *neo4jRelationRepo) DeleteRelation(ctx context.Context, id string) error {
	// TODO: 重构 DeleteRelation
	fmt.Printf("TODO: Repo 实现 DeleteRelation, id: %s\n", id)
	return fmt.Errorf("Repo: DeleteRelation 未实现")
}

func (r *neo4jRelationRepo) GetNodeRelations(ctx context.Context, req *network.GetNodeRelationsRequest) ([]*network.Relation, int32, error) {
	// TODO: 重构 GetNodeRelations
	fmt.Printf("TODO: Repo 实现 GetNodeRelations, node_id: %s\n", req.NodeID)
	return nil, 0, fmt.Errorf("Repo: GetNodeRelations 未实现")
}
