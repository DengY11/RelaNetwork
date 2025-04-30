package neo4jrepo

import (
	"context"
	"fmt"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/dbtype"

	network "labelwall/biz/model/relationship/network" // 确保路径正确
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
	driver neo4j.DriverWithContext
}

// NewRelationRepository 创建一个新的 RelationRepository 实例
func NewRelationRepository(driver neo4j.DriverWithContext) RelationRepository {
	return &neo4jRelationRepo{driver: driver}
}

// CreateRelation 在 Neo4j 中创建两个节点之间的新关系
func (r *neo4jRelationRepo) CreateRelation(ctx context.Context, req *network.CreateRelationRequest) (*network.Relation, error) {
	session := r.driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)

	// 生成唯一关系ID
	relationID := fmt.Sprintf("%s-%s-%d", req.Source, req.Target, time.Now().UnixNano())

	relResult, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		// 构建关系属性Map
		properties := map[string]any{
			"id":    relationID, // 关系业务 ID
			"label": req.Label,
		}
		if req.Properties != nil {
			for k, v := range req.Properties {
				properties[k] = v // 合并可选属性
			}
		}

		// 构建 Cypher 查询
		// 使用参数化查询匹配源节点和目标节点
		// 关系类型使用 RelationType 的字符串表示
		query := fmt.Sprintf(`
            MATCH (source {id: $sourceId}), (target {id: $targetId})
            CREATE (source)-[rel:%s $props]->(target)
            RETURN rel, source.id AS sourceId, target.id AS targetId`, // 使用占位符确保类型正确
			req.Type.String(), // RelationType -> RELATION_TYPE (e.g., COLLEAGUE)
		)

		result, err := tx.Run(ctx, query, map[string]any{
			"sourceId": req.Source,
			"targetId": req.Target,
			"props":    properties,
		})
		if err != nil {
			return nil, fmt.Errorf("运行创建关系查询失败: %w", err)
		}

		record, err := result.Single(ctx)
		if err != nil {
			// 可能是节点未找到，需要更具体的错误处理
			if neo4j.IsNeo4jError(err) {
				// 检查特定的错误代码，例如节点不存在等
			}
			return nil, fmt.Errorf("获取创建关系结果失败: %w", err)
		}

		relInterface, ok := record.Get("rel")
		if !ok {
			return nil, fmt.Errorf("无法从结果中获取关系 'rel'")
		}

		dbRel, ok := relInterface.(dbtype.Relationship)
		if !ok {
			return nil, fmt.Errorf("结果中的 'rel' 不是有效的关系类型")
		}

		// 从记录中获取实际匹配到的 sourceId 和 targetId
		sourceIdVal, _ := record.Get("sourceId")
		targetIdVal, _ := record.Get("targetId")

		// 将 Neo4j 关系映射回 Thrift 关系结构
		return mapDbRelationshipToThriftRelation(dbRel, req.Type, sourceIdVal.(string), targetIdVal.(string)), nil
	})

	if err != nil {
		return nil, err
	}

	createdRelation, ok := relResult.(*network.Relation)
	if !ok {
		return nil, fmt.Errorf("事务返回了非预期的关系类型")
	}

	return createdRelation, nil
}

// mapDbRelationshipToThriftRelation 将 Neo4j 关系对象转换为 Thrift Relation 对象
// 需要传入 RelationType, source ID 和 target ID，因为基础 dbtype.Relationship 不包含这些
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

	// 填充其他属性
	coreProps := map[string]struct{}{"id": {}, "label": {}}
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

// --- 其他方法的占位实现 ---

func (r *neo4jRelationRepo) GetRelation(ctx context.Context, id string) (*network.Relation, error) {
	// TODO: 实现 GetRelation 查询
	fmt.Printf("TODO: 实现 GetRelation 查询, id: %s\n", id)
	return nil, fmt.Errorf("GetRelation 未实现")
}

func (r *neo4jRelationRepo) UpdateRelation(ctx context.Context, req *network.UpdateRelationRequest) (*network.Relation, error) {
	// TODO: 实现 UpdateRelation 查询
	fmt.Printf("TODO: 实现 UpdateRelation 查询, id: %s\n", req.ID)
	return nil, fmt.Errorf("UpdateRelation 未实现")
}

func (r *neo4jRelationRepo) DeleteRelation(ctx context.Context, id string) error {
	// TODO: 实现 DeleteRelation 查询
	fmt.Printf("TODO: 实现 DeleteRelation 查询, id: %s\n", id)
	return fmt.Errorf("DeleteRelation 未实现")
}

func (r *neo4jRelationRepo) GetNodeRelations(ctx context.Context, req *network.GetNodeRelationsRequest) ([]*network.Relation, int32, error) {
	// TODO: 实现 GetNodeRelations 查询
	fmt.Printf("TODO: 实现 GetNodeRelations 查询, node_id: %s\n", req.NodeID)
	return nil, 0, fmt.Errorf("GetNodeRelations 未实现")
}
