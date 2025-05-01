package neo4jdal

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	network "labelwall/biz/model/relationship/network"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/dbtype"
)

// neo4jRelationDAL 实现了 RelationDAL 接口，封装关系相关的底层数据库操作。
type neo4jRelationDAL struct {
}

// NewRelationDAL 创建一个新的 RelationDAL 实例。
// 返回 RelationDAL 接口类型，供repo层使用。
func NewRelationDAL() RelationDAL {
	return &neo4jRelationDAL{}
}

// ExecCreateRelation 执行创建关系的 Cypher 语句。
// properties map 由 Repo 层构建，包含 id 和所有关系属性。
func (d *neo4jRelationDAL) ExecCreateRelation(ctx context.Context, session neo4j.SessionWithContext, sourceID, targetID string, relType network.RelationType, properties map[string]any) (neo4j.Relationship, error) {
	// 执行写事务。
	relResult, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		// 构建查询语句，匹配源节点和目标节点，然后创建带有类型和属性的关系。
		query := fmt.Sprintf(`
            MATCH (source {id: $sourceId}), (target {id: $targetId})
            CREATE (source)-[rel:%s $props]->(target)
            RETURN rel`, relType.String()) // 使用关系类型的字符串表示

		// 执行查询。
		result, err := tx.Run(ctx, query, map[string]any{
			"sourceId": sourceID,
			"targetId": targetID,
			"props":    properties,
		})
		if err != nil {
			return nil, fmt.Errorf("DAL: 运行创建关系查询失败: %w", err)
		}

		// 获取单个结果记录。
		record, err := result.Single(ctx)
		if err != nil {
			// 如果节点未找到，MATCH 会失败，导致 Single() 出错。
			return nil, fmt.Errorf("DAL: 获取创建关系结果失败 (可能是节点不存在): %w", err)
		}

		// 提取关系数据。
		relInterface, ok := record.Get("rel")
		if !ok {
			return nil, fmt.Errorf("DAL: 无法从结果中获取关系 'rel'")
		}

		// 类型断言为 Neo4j 关系类型。
		dbRel, ok := relInterface.(dbtype.Relationship)
		if !ok {
			return nil, fmt.Errorf("DAL: 结果中的 'rel' 不是有效的关系类型")
		}
		// 直接返回 Neo4j 驱动的关系类型。
		return dbRel, nil
	})

	// 处理事务执行错误。
	if err != nil {
		return dbtype.Relationship{}, err // 返回零值关系和错误。
	}

	// 类型断言事务返回的结果。
	createdRel, ok := relResult.(dbtype.Relationship)
	if !ok {
		return dbtype.Relationship{}, fmt.Errorf("DAL: 事务返回了非预期的关系类型")
	}
	return createdRel, nil
}

// ExecGetRelationByID 执行按 ID 获取关系的 Cypher。
// 返回关系本身、类型字符串、源节点 ID 和目标节点 ID。
func (d *neo4jRelationDAL) ExecGetRelationByID(ctx context.Context, session neo4j.SessionWithContext, id string) (dbtype.Relationship, string, string, string, error) {
	// 执行读事务。
	readResult, err := session.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		// 查询匹配关系 ID 的关系，并返回关系本身、类型、源节点 ID、目标节点 ID。
		query := `MATCH (s)-[r {id: $id}]->(t) RETURN r, type(r) as type, s.id as sourceId, t.id as targetId`
		result, err := tx.Run(ctx, query, map[string]any{"id": id})
		if err != nil {
			return nil, fmt.Errorf("DAL: 运行获取关系查询失败: %w", err)
		}
		// 获取单个结果。
		record, err := result.Single(ctx)
		if err != nil {
			// 检查是否为"未找到"错误。
			usageErr := new(neo4j.UsageError)
			if errors.As(err, &usageErr) && strings.Contains(usageErr.Error(), "result contains no more records") {
				return nil, nil // 返回 nil, nil 表示未找到。
			}
			return nil, fmt.Errorf("DAL: 获取关系结果失败: %w", err)
		}
		// 提取结果字段。
		relInterface, _ := record.Get("r")
		typeInterface, _ := record.Get("type")
		sourceIdInterface, _ := record.Get("sourceId")
		targetIdInterface, _ := record.Get("targetId")

		// 类型断言关系。
		dbRel, ok := relInterface.(dbtype.Relationship)
		if !ok {
			return nil, fmt.Errorf("DAL: 结果中的 'r' 不是有效的关系类型")
		}

		// 返回包含所有信息的 map。
		return map[string]any{
			"rel":      dbRel,
			"type":     typeInterface.(string),
			"sourceId": sourceIdInterface.(string),
			"targetId": targetIdInterface.(string),
		}, nil
	})
	// 处理事务错误。
	if err != nil {
		return dbtype.Relationship{}, "", "", "", err
	}
	// 处理"未找到"情况。
	if readResult == nil {
		return dbtype.Relationship{}, "", "", "", nil // 返回零值和 nil 错误表示未找到。
	}
	// 解析结果 map 并返回。
	resultMap := readResult.(map[string]any)
	return resultMap["rel"].(dbtype.Relationship),
		resultMap["type"].(string),
		resultMap["sourceId"].(string),
		resultMap["targetId"].(string),
		nil
}

// ExecUpdateRelation 执行更新关系的 Cypher。
// updates map 由 Repo 层准备。
func (d *neo4jRelationDAL) ExecUpdateRelation(ctx context.Context, session neo4j.SessionWithContext, id string, updates map[string]any) (dbtype.Relationship, string, string, string, error) {
	// 执行写事务。
	writeResult, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		// 动态构建 SET 子句。
		setClauses := []string{}
		params := map[string]any{"id": id}
		for key, value := range updates {
			paramName := "update_" + key
			setClauses = append(setClauses, fmt.Sprintf("r.%s = $%s", key, paramName))
			params[paramName] = value
		}
		// 自动更新 updated_at。
		setClauses = append(setClauses, "r.updated_at = $now")
		params["now"] = time.Now().UTC()

		// 构建更新查询语句。
		query := fmt.Sprintf(`MATCH (s)-[r {id: $id}]->(t) SET %s RETURN r, type(r) as type, s.id as sourceId, t.id as targetId`, strings.Join(setClauses, ", "))

		// 执行查询。
		result, err := tx.Run(ctx, query, params)
		if err != nil {
			return nil, fmt.Errorf("DAL: 运行更新关系查询失败: %w", err)
		}
		// 获取单个结果。
		record, err := result.Single(ctx)
		if err != nil {
			// 可能是关系未找到。
			return nil, fmt.Errorf("DAL: 获取更新关系结果失败: %w", err)
		}
		// 解析结果（同 GetRelationByID）。
		relInterface, _ := record.Get("r")
		typeInterface, _ := record.Get("type")
		sourceIdInterface, _ := record.Get("sourceId")
		targetIdInterface, _ := record.Get("targetId")
		dbRel, ok := relInterface.(dbtype.Relationship)
		if !ok {
			return nil, fmt.Errorf("DAL: 结果中的 'r' 不是有效的关系类型")
		}
		return map[string]any{
			"rel":      dbRel,
			"type":     typeInterface.(string),
			"sourceId": sourceIdInterface.(string),
			"targetId": targetIdInterface.(string),
		}, nil
	})
	// 处理事务错误。
	if err != nil {
		return dbtype.Relationship{}, "", "", "", err
	}
	// 解析结果 map。
	resultMap := writeResult.(map[string]any)
	return resultMap["rel"].(dbtype.Relationship),
		resultMap["type"].(string),
		resultMap["sourceId"].(string),
		resultMap["targetId"].(string),
		nil
}

// ExecDeleteRelation 执行删除关系的 Cypher。
// 返回错误信息，如果关系未找到则报错。
func (d *neo4jRelationDAL) ExecDeleteRelation(ctx context.Context, session neo4j.SessionWithContext, id string) error {
	// 执行写事务。
	resultSummary, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		// 匹配并删除指定 ID 的关系。
		query := `MATCH ()-[r {id: $id}]->() DELETE r`
		result, err := tx.Run(ctx, query, map[string]any{"id": id})
		if err != nil {
			return nil, fmt.Errorf("query execution failed: %w", err)
		}
		// 获取并返回结果摘要，用于检查删除计数
		summary, err := result.Consume(ctx)
		if err != nil {
			return nil, fmt.Errorf("result consumption failed: %w", err)
		}
		return summary, nil
	})
	// 处理事务错误。
	if err != nil {
		return fmt.Errorf("DAL: 删除关系事务失败: %w", err)
	}

	// 事务成功后，检查结果摘要。
	if summary, ok := resultSummary.(neo4j.ResultSummary); ok {
		// 检查计数器中被删除的关系数量。
		if summary.Counters().RelationshipsDeleted() == 0 {
			// 如果没有关系被删除，说明具有该 ID 的关系不存在。
			return fmt.Errorf("DAL: relation with id '%s' not found for deletion", id)
		}
		// 关系删除成功。
		return nil
	}

	// 如果 ExecuteWrite 成功但返回的不是预期的 ResultSummary 类型。
	return fmt.Errorf("DAL: 删除关系事务返回了非预期的结果类型")
}

// ExecGetNodeRelations 执行获取特定节点所有关系的 Cypher。
// 支持按类型、方向、分页进行过滤。
func (d *neo4jRelationDAL) ExecGetNodeRelations(ctx context.Context, session neo4j.SessionWithContext, nodeID string, types []string, outgoing, incoming bool, limit, offset int64) ([]dbtype.Relationship, []string, []string, []string, int64, error) {
	// 初始化返回值。
	rels := []dbtype.Relationship{}
	relTypes := []string{}
	sourceIDs := []string{}
	targetIDs := []string{}
	var total int64 = 0

	// 执行读事务。
	readResult, err := session.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		params := map[string]any{ // 初始化查询参数
			"nodeId": nodeID,
			"limit":  limit,
			"offset": offset,
			"types":  types, // 类型列表，空 slice 表示所有类型
		}

		// 动态构建 MATCH 和 WHERE 子句。
		var matchBuilder, whereBuilder strings.Builder
		matchBuilder.WriteString("MATCH (n {id: $nodeId})")
		// 根据方向构建匹配模式。
		if outgoing && incoming {
			matchBuilder.WriteString("-[r]-(neighbor)") // 双向
		} else if outgoing {
			matchBuilder.WriteString("-[r]->(neighbor)") // 出向
		} else if incoming {
			matchBuilder.WriteString("<-[r]-(neighbor)") // 入向
		} else {
			// 如果没有指定方向，则不进行查询，直接返回空结果。
			return map[string]any{"rels": rels, "types": relTypes, "sourceIds": sourceIDs, "targetIds": targetIDs, "total": total}, nil
		}

		// 构建 WHERE 子句，用于类型过滤。
		whereBuilder.WriteString(" WHERE (size($types) = 0 OR type(r) IN $types)")

		// --- 第一步：获取总数 ---
		countQuery := matchBuilder.String() + whereBuilder.String() + " RETURN count(r) AS total"
		countResult, err := tx.Run(ctx, countQuery, params)
		if err != nil {
			return nil, fmt.Errorf("DAL: 运行获取节点关系总数查询失败: %w", err)
		}
		// 获取总数结果。
		countRecord, err := countResult.Single(ctx)
		if err != nil {
			// 处理没有关系的情况。
			usageErr := new(neo4j.UsageError)
			if errors.As(err, &usageErr) && strings.Contains(usageErr.Error(), "result contains no more records") {
				total = 0
			} else {
				return nil, fmt.Errorf("DAL: 获取节点关系总数失败: %w", err)
			}
		} else {
			totalVal, _ := countRecord.Get("total")
			total = totalVal.(int64)
		}

		// 如果总数为 0，直接返回。
		if total == 0 {
			return map[string]any{"rels": rels, "types": relTypes, "sourceIds": sourceIDs, "targetIds": targetIDs, "total": total}, nil
		}

		// --- 第二步：获取分页后的关系数据 ---
		// 根据查询方向确定如何获取 sourceId 和 targetId。
		var returnClause string
		if outgoing && incoming {
			// 双向查询时，使用 startNode 和 endNode 获取。
			returnClause = "RETURN r, type(r) as type, startNode(r).id as sourceId, endNode(r).id as targetId"
		} else if outgoing {
			// 出向查询时，源节点是 $nodeId。
			returnClause = "RETURN r, type(r) as type, $nodeId as sourceId, neighbor.id as targetId"
		} else { // incoming
			// 入向查询时，目标节点是 $nodeId。
			returnClause = "RETURN r, type(r) as type, neighbor.id as sourceId, $nodeId as targetId"
		}
		// 构建完整的数据查询语句，添加排序和分页。
		dataQuery := matchBuilder.String() + whereBuilder.String() + returnClause + " ORDER BY r.created_at DESC SKIP $offset LIMIT $limit" // 示例：按创建时间降序排序

		// 执行数据查询。
		dataResult, err := tx.Run(ctx, dataQuery, params)
		if err != nil {
			return nil, fmt.Errorf("DAL: 运行获取节点关系数据查询失败: %w", err)
		}
		// 收集所有结果记录。
		records, err := dataResult.Collect(ctx)
		if err != nil {
			return nil, fmt.Errorf("DAL: 收集获取节点关系结果失败: %w", err)
		}

		// 遍历记录，提取所需信息。
		rels = make([]dbtype.Relationship, len(records))
		relTypes = make([]string, len(records))
		sourceIDs = make([]string, len(records))
		targetIDs = make([]string, len(records))
		for i, record := range records {
			relInterface, _ := record.Get("r")
			typeInterface, _ := record.Get("type")
			sourceIdInterface, _ := record.Get("sourceId")
			targetIdInterface, _ := record.Get("targetId")
			rels[i] = relInterface.(dbtype.Relationship)
			relTypes[i] = typeInterface.(string)
			sourceIDs[i] = sourceIdInterface.(string)
			targetIDs[i] = targetIdInterface.(string)
		}
		// 返回包含所有信息的 map。
		return map[string]any{"rels": rels, "types": relTypes, "sourceIds": sourceIDs, "targetIds": targetIDs, "total": total}, nil
	})

	// 处理事务错误。
	if err != nil {
		return nil, nil, nil, nil, 0, err
	}
	// 解析事务返回的 map。
	resultMap := readResult.(map[string]any)
	return resultMap["rels"].([]dbtype.Relationship),
		resultMap["types"].([]string),
		resultMap["sourceIds"].([]string),
		resultMap["targetIds"].([]string),
		resultMap["total"].(int64),
		nil
}
