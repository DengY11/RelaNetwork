package neo4jrepo

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"

	"labelwall/biz/dal/neo4jdal"
	network "labelwall/biz/model/relationship/network"
	// TODO "labelwall/pkg/cache" // 缓存包（待实现）
)

// neo4jRelationRepo 实现了 RelationRepository 接口
type neo4jRelationRepo struct {
	driver      neo4j.DriverWithContext
	relationDAL neo4jdal.RelationDAL
	// cache       cache.RelationCache // 关系缓存接口（待实现）
}

// NewRelationRepository 创建一个新的 RelationRepository 实例
func NewRelationRepository(driver neo4j.DriverWithContext, relationDAL neo4jdal.RelationDAL /*, cache cache.RelationCache*/) RelationRepository {
	return &neo4jRelationRepo{
		driver:      driver,
		relationDAL: relationDAL,
		// TODO cache:       cache,
	}
}

// CreateRelation 创建一个新的关系
func (r *neo4jRelationRepo) CreateRelation(ctx context.Context, req *network.CreateRelationRequest) (*network.Relation, error) {
	session := r.driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)

	// 1. 生成唯一业务 ID
	relationID := uuid.NewString()

	// 2. 构建关系属性 Map
	properties := map[string]any{
		"id":         relationID,
		"created_at": time.Now().UTC(),
		"updated_at": time.Now().UTC(),
	}
	if req.Label != nil {
		properties["label"] = *req.Label
	}
	if req.Properties != nil {
		for k, v := range req.Properties {
			if _, exists := properties[k]; !exists {
				properties[k] = v
			}
		}
	}

	// 3. 调用 DAL 层执行创建
	// ExecCreateRelation 期望返回创建的关系及其类型
	dbRel, err := r.relationDAL.ExecCreateRelation(ctx, session, req.Source, req.Target, req.Type, properties)
	if err != nil {
		return nil, fmt.Errorf("repo: 调用 DAL 创建关系失败: %w", err)
	}

	// 4. 映射结果
	// mapDbRelationshipToThriftRelation 需要源和目标 ID，这里直接用请求里的
	return mapDbRelationshipToThriftRelation(dbRel, req.Type, req.Source, req.Target), nil
}

// GetRelation 通过 ID 获取关系
func (r *neo4jRelationRepo) GetRelation(ctx context.Context, id string) (*network.Relation, error) {
	/*
		// 1. 尝试从缓存获取
		if r.cache != nil {
			cachedRel, err := r.cache.Get(ctx, id)
			if err == nil && cachedRel != nil {
				fmt.Printf("Repo: GetRelation cache hit for id: %s\n", id)
				return cachedRel, nil
			}
			if err != nil && !errors.Is(err, cache.ErrNotFound) {
				fmt.Printf("WARN: Repo: 缓存获取关系失败 (id: %s): %v\n", id, err)
			}
		}
	*/

	// 2. 从数据库获取
	session := r.driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
	defer session.Close(ctx)

	// ExecGetRelationByID 期望返回关系、类型字符串、源节点ID、目标节点ID
	dbRel, relTypeStr, sourceID, targetID, err := r.relationDAL.ExecGetRelationByID(ctx, session, id)
	if err != nil {
		if isNotFoundError(err) {
			return nil, err // 透传 Not Found
		}
		return nil, fmt.Errorf("repo: 调用 DAL 获取关系失败: %w", err)
	}

	// 3. 映射结果
	relType, ok := stringToRelationType(relTypeStr)
	if !ok {
		fmt.Printf("WARN: Repo: 无法识别关系 (id: %s) 的类型: %s\n", id, relTypeStr)
		return nil, fmt.Errorf("repo: 无法识别的关系类型 %s", relTypeStr)
	}
	resultRel := mapDbRelationshipToThriftRelation(dbRel, relType, sourceID, targetID)

	/*
		// 4. 存入缓存
		if r.cache != nil && resultRel != nil {
			setErr := r.cache.Set(ctx, id, resultRel, time.Hour)
			if setErr != nil {
				fmt.Printf("WARN: Repo: 缓存设置关系失败 (id: %s): %v\n", id, setErr)
			}
		}
	*/

	return resultRel, nil
}

// UpdateRelation 更新关系属性
func (r *neo4jRelationRepo) UpdateRelation(ctx context.Context, req *network.UpdateRelationRequest) (*network.Relation, error) {
	session := r.driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)

	// 1. 构建更新 Map
	updates := map[string]any{
		"updated_at": time.Now().UTC(),
	}
	if req.Label != nil {
		updates["label"] = *req.Label
	}
	if req.Properties != nil {
		for k, v := range req.Properties {
			if k != "id" && k != "created_at" && k != "updated_at" {
				updates[k] = v
			}
		}
	}

	// 2. 调用 DAL 层执行更新
	// ExecUpdateRelation 返回更新后的关系、类型字符串、源和目标 ID
	// 注意：DAL 层不接受类型更新作为参数
	dbRel, relTypeStr, sourceID, targetID, err := r.relationDAL.ExecUpdateRelation(ctx, session, req.ID, updates)
	if err != nil {
		if isNotFoundError(err) {
			return nil, err // 透传 Not Found
		}
		return nil, fmt.Errorf("repo: 调用 DAL 更新关系失败: %w", err)
	}

	// 3. 映射结果
	relType, ok := stringToRelationType(relTypeStr)
	if !ok {
		fmt.Printf("WARN: Repo: 更新后无法识别关系 (id: %s) 的类型: %s\n", req.ID, relTypeStr)
		return nil, fmt.Errorf("repo: 更新后无法识别的关系类型 %s", relTypeStr)
	}
	updatedRel := mapDbRelationshipToThriftRelation(dbRel, relType, sourceID, targetID)

	/*
		// 4. 使缓存失效
		if r.cache != nil {
			delErr := r.cache.Delete(ctx, req.ID)
			if delErr != nil && !errors.Is(delErr, cache.ErrNotFound) {
				fmt.Printf("WARN: Repo: 缓存删除关系失败 (id: %s): %v\n", req.ID, delErr)
			}
		}
	*/

	return updatedRel, nil
}

// DeleteRelation 删除关系
func (r *neo4jRelationRepo) DeleteRelation(ctx context.Context, id string) error {
	session := r.driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)

	// 1. 调用 DAL 层执行删除
	err := r.relationDAL.ExecDeleteRelation(ctx, session, id)
	if err != nil {
		if isNotFoundError(err) {
			return err // 透传 Not Found
		}
		return fmt.Errorf("repo: 调用 DAL 删除关系失败: %w", err)
	}

	/*
		// 2. 使缓存失效
		if r.cache != nil {
			delErr := r.cache.Delete(ctx, id)
			if delErr != nil && !errors.Is(delErr, cache.ErrNotFound) {
				fmt.Printf("WARN: Repo: 缓存删除关系失败 (id: %s): %v\n", id, delErr)
			}
		}
	*/

	return nil
}

// GetNodeRelations 获取节点的关系列表
func (r *neo4jRelationRepo) GetNodeRelations(ctx context.Context, req *network.GetNodeRelationsRequest) ([]*network.Relation, int32, error) {
	session := r.driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
	defer session.Close(ctx)

	// 1. 处理可选参数
	var relTypesStr []string
	if req.IsSetTypes() && len(req.Types) > 0 {
		relTypesStr = make([]string, 0, len(req.Types))
		for _, rt := range req.Types {
			relTypesStr = append(relTypesStr, rt.String())
		}
	}

	outgoing := true // 默认获取出向关系
	if req.IsSetOutgoing() {
		outgoing = *req.Outgoing
	}
	incoming := true // 默认获取入向关系
	if req.IsSetIncoming() {
		incoming = *req.Incoming
	}

	var limit, offset int64
	if req.IsSetLimit() {
		limit = int64(*req.Limit)
	} else {
		limit = 10 // 默认分页大小
	}
	if req.IsSetOffset() {
		offset = int64(*req.Offset)
	} else {
		offset = 0
	}

	// 2. 调用 DAL 获取关系数据
	// ExecGetNodeRelations 返回 关系列表, 类型字符串列表, 源ID列表, 目标ID列表, 总数, 错误
	dbRels, relTypeStrs, sourceIDs, targetIDs, total, err := r.relationDAL.ExecGetNodeRelations(ctx, session, req.NodeID, relTypesStr, outgoing, incoming, limit, offset)
	if err != nil {
		return nil, 0, fmt.Errorf("repo: 调用 DAL 获取节点关系失败: %w", err)
	}

	// 3. 映射结果
	resultRelations := make([]*network.Relation, 0, len(dbRels))
	for i, dbRel := range dbRels {
		relTypeStr := relTypeStrs[i]
		sourceID := sourceIDs[i]
		targetID := targetIDs[i]

		relType, ok := stringToRelationType(relTypeStr)
		if !ok {
			relID := getStringProp(dbRel.Props, "id", "[未知ID]")
			fmt.Printf("WARN: Repo: GetNodeRelations 中无法识别关系 (id: %s) 的类型: %s\n", relID, relTypeStr)
			continue // 跳过无法识别类型的关系
		}

		thriftRelation := mapDbRelationshipToThriftRelation(dbRel, relType, sourceID, targetID)
		if thriftRelation != nil {
			resultRelations = append(resultRelations, thriftRelation)
		}
	}

	return resultRelations, int32(total), nil
}
