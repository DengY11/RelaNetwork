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

// neo4jRelationDAL 实现了 RelationDAL 接口
type neo4jRelationDAL struct {
}

// NewRelationDAL 创建一个新的 RelationDAL 实例
func NewRelationDAL() RelationDAL {
	return &neo4jRelationDAL{}
}

// ExecCreateRelation 执行创建关系的 Cypher 语句
func (d *neo4jRelationDAL) ExecCreateRelation(ctx context.Context, session neo4j.SessionWithContext, sourceID, targetID string, relType network.RelationType, properties map[string]any) (neo4j.Relationship, error) {
	relResult, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		query := fmt.Sprintf(`
            MATCH (source {id: $sourceId}), (target {id: $targetId})
            CREATE (source)-[rel:%s $props]->(target)
            RETURN rel`, relType.String())

		result, err := tx.Run(ctx, query, map[string]any{
			"sourceId": sourceID,
			"targetId": targetID,
			"props":    properties,
		})
		if err != nil {
			return nil, fmt.Errorf("DAL: 运行创建关系查询失败: %w", err)
		}

		record, err := result.Single(ctx)
		if err != nil {
			return nil, fmt.Errorf("DAL: 获取创建关系结果失败 (可能是节点不存在): %w", err)
		}

		relInterface, ok := record.Get("rel")
		if !ok {
			return nil, fmt.Errorf("DAL: 无法从结果中获取关系 'rel'")
		}

		dbRel, ok := relInterface.(dbtype.Relationship)
		if !ok {
			return nil, fmt.Errorf("DAL: 结果中的 'rel' 不是有效的关系类型")
		}
		return dbRel, nil
	})

	if err != nil {
		return dbtype.Relationship{}, err
	}

	createdRel, ok := relResult.(dbtype.Relationship)
	if !ok {
		return dbtype.Relationship{}, fmt.Errorf("DAL: 事务返回了非预期的关系类型")
	}
	return createdRel, nil
}

// ExecGetRelationByID 执行按 ID 获取关系的 Cypher
func (d *neo4jRelationDAL) ExecGetRelationByID(ctx context.Context, session neo4j.SessionWithContext, id string) (dbtype.Relationship, string, string, string, error) {
	readResult, err := session.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		query := `MATCH (s)-[r {id: $id}]->(t) RETURN r, type(r) as type, s.id as sourceId, t.id as targetId`
		result, err := tx.Run(ctx, query, map[string]any{"id": id})
		if err != nil {
			return nil, fmt.Errorf("DAL: 运行获取关系查询失败: %w", err)
		}
		record, err := result.Single(ctx)
		if err != nil {
			usageErr := new(neo4j.UsageError)
			if errors.As(err, &usageErr) && strings.Contains(usageErr.Error(), "result contains no more records") {
				return nil, nil // 未找到
			}
			return nil, fmt.Errorf("DAL: 获取关系结果失败: %w", err)
		}
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
	if err != nil {
		return dbtype.Relationship{}, "", "", "", err
	}
	if readResult == nil {
		return dbtype.Relationship{}, "", "", "", nil // 未找到
	}
	resultMap := readResult.(map[string]any)
	return resultMap["rel"].(dbtype.Relationship),
		resultMap["type"].(string),
		resultMap["sourceId"].(string),
		resultMap["targetId"].(string),
		nil
}

// ExecUpdateRelation 执行更新关系的 Cypher
func (d *neo4jRelationDAL) ExecUpdateRelation(ctx context.Context, session neo4j.SessionWithContext, id string, updates map[string]any) (dbtype.Relationship, string, string, string, error) {
	writeResult, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		// 构建 SET 子句
		setClauses := []string{}
		params := map[string]any{"id": id}
		for key, value := range updates {
			paramName := "update_" + key
			setClauses = append(setClauses, fmt.Sprintf("r.%s = $%s", key, paramName))
			params[paramName] = value
		}
		setClauses = append(setClauses, "r.updated_at = $now")
		params["now"] = time.Now().UTC()

		query := fmt.Sprintf(`MATCH (s)-[r {id: $id}]->(t) SET %s RETURN r, type(r) as type, s.id as sourceId, t.id as targetId`, strings.Join(setClauses, ", "))

		result, err := tx.Run(ctx, query, params)
		if err != nil {
			return nil, fmt.Errorf("DAL: 运行更新关系查询失败: %w", err)
		}
		record, err := result.Single(ctx)
		if err != nil {
			return nil, fmt.Errorf("DAL: 获取更新关系结果失败: %w", err)
		}
		// ... (结果解析同 GetRelationByID)
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
	if err != nil {
		return dbtype.Relationship{}, "", "", "", err
	}
	resultMap := writeResult.(map[string]any)
	return resultMap["rel"].(dbtype.Relationship),
		resultMap["type"].(string),
		resultMap["sourceId"].(string),
		resultMap["targetId"].(string),
		nil
}

// ExecDeleteRelation 执行删除关系的 Cypher
func (d *neo4jRelationDAL) ExecDeleteRelation(ctx context.Context, session neo4j.SessionWithContext, id string) error {
	_, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		query := `MATCH ()-[r {id: $id}]->() DELETE r`
		_, err := tx.Run(ctx, query, map[string]any{"id": id})
		// 可以检查 ResultSummary().Counters().RelationshipsDeleted()
		return nil, err
	})
	if err != nil {
		return fmt.Errorf("DAL: 运行删除关系查询失败: %w", err)
	}
	return nil
}

// ExecGetNodeRelations 执行获取节点关系的 Cypher
func (d *neo4jRelationDAL) ExecGetNodeRelations(ctx context.Context, session neo4j.SessionWithContext, nodeID string, types []string, outgoing, incoming bool, limit, offset int64) ([]dbtype.Relationship, []string, []string, []string, int64, error) {
	rels := []dbtype.Relationship{}
	relTypes := []string{}
	sourceIDs := []string{}
	targetIDs := []string{}
	var total int64 = 0

	readResult, err := session.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		params := map[string]any{
			"nodeId": nodeID,
			"limit":  limit,
			"offset": offset,
			"types":  types, // 空 slice 表示匹配所有类型
		}

		// 构建 MATCH 和 WHERE 子句
		var matchBuilder, whereBuilder strings.Builder
		matchBuilder.WriteString(fmt.Sprintf("MATCH (n {id: $nodeId})"))
		if outgoing && incoming {
			matchBuilder.WriteString("-[r]-(neighbor)")
		} else if outgoing {
			matchBuilder.WriteString("-[r]->(neighbor)")
		} else if incoming {
			matchBuilder.WriteString("<-[r]-(neighbor)")
		} else {
			return map[string]any{"rels": rels, "types": relTypes, "sourceIds": sourceIDs, "targetIds": targetIDs, "total": total}, nil // 没有方向，不查询
		}

		whereBuilder.WriteString(" WHERE (size($types) = 0 OR type(r) IN $types)")

		// --- 获取总数 ---
		countQuery := matchBuilder.String() + whereBuilder.String() + " RETURN count(r) AS total"
		countResult, err := tx.Run(ctx, countQuery, params)
		if err != nil {
			return nil, fmt.Errorf("DAL: 运行获取节点关系总数查询失败: %w", err)
		}
		countRecord, err := countResult.Single(ctx)
		if err != nil {
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

		if total == 0 {
			return map[string]any{"rels": rels, "types": relTypes, "sourceIds": sourceIDs, "targetIds": targetIDs, "total": total}, nil
		}

		// --- 获取关系数据 ---
		// 需要根据方向确定 source 和 target
		var returnClause string
		if outgoing && incoming {
			returnClause = "RETURN r, type(r) as type, startNode(r).id as sourceId, endNode(r).id as targetId"
		} else if outgoing {
			returnClause = "RETURN r, type(r) as type, $nodeId as sourceId, neighbor.id as targetId"
		} else { // incoming
			returnClause = "RETURN r, type(r) as type, neighbor.id as sourceId, $nodeId as targetId"
		}
		dataQuery := matchBuilder.String() + whereBuilder.String() + returnClause + " ORDER BY r.created_at DESC SKIP $offset LIMIT $limit" // 按创建时间排序示例

		dataResult, err := tx.Run(ctx, dataQuery, params)
		if err != nil {
			return nil, fmt.Errorf("DAL: 运行获取节点关系数据查询失败: %w", err)
		}
		records, err := dataResult.Collect(ctx)
		if err != nil {
			return nil, fmt.Errorf("DAL: 收集获取节点关系结果失败: %w", err)
		}

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
		return map[string]any{"rels": rels, "types": relTypes, "sourceIds": sourceIDs, "targetIds": targetIDs, "total": total}, nil
	})

	if err != nil {
		return nil, nil, nil, nil, 0, err
	}
	resultMap := readResult.(map[string]any)
	return resultMap["rels"].([]dbtype.Relationship),
		resultMap["types"].([]string),
		resultMap["sourceIds"].([]string),
		resultMap["targetIds"].([]string),
		resultMap["total"].(int64),
		nil
}
