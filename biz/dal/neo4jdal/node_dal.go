package neo4jdal

import (
	"context"
	"fmt"
	"strings"
	"time"

	network "labelwall/biz/model/relationship/network"

	"errors"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/dbtype"
)

// neo4jNodeDAL 实现了 NodeDAL 接口
type neo4jNodeDAL struct {
	// 可以持有通用配置或辅助工具，但通常不直接持有 driver
}

// NewNodeDAL 创建一个新的 NodeDAL 实例
func NewNodeDAL() NodeDAL { // 返回接口类型
	return &neo4jNodeDAL{}
}

// ExecCreateNode 执行创建节点的 Cypher 语句
func (d *neo4jNodeDAL) ExecCreateNode(ctx context.Context, session neo4j.SessionWithContext, nodeType network.NodeType, properties map[string]any) (neo4j.Node, error) {
	nodeResult, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		query := fmt.Sprintf(`CREATE (n:%s $props) RETURN n`, nodeType.String())
		result, err := tx.Run(ctx, query, map[string]any{"props": properties})
		if err != nil {
			return nil, fmt.Errorf("DAL: 运行创建节点查询失败: %w", err)
		}
		record, err := result.Single(ctx)
		if err != nil {
			return nil, fmt.Errorf("DAL: 获取创建节点结果失败: %w", err)
		}
		nodeInterface, ok := record.Get("n")
		if !ok {
			return nil, fmt.Errorf("DAL: 无法从结果中获取节点 'n'")
		}
		dbNode, ok := nodeInterface.(dbtype.Node)
		if !ok {
			return nil, fmt.Errorf("DAL: 结果中的 'n' 不是有效的节点类型")
		}
		return dbNode, nil
	})
	if err != nil {
		return dbtype.Node{}, err
	}
	createdNode, ok := nodeResult.(dbtype.Node)
	if !ok {
		return dbtype.Node{}, fmt.Errorf("DAL: 事务返回了非预期的节点类型")
	}
	return createdNode, nil
}

// ExecGetNodeByID 执行按 ID 获取节点的 Cypher
func (d *neo4jNodeDAL) ExecGetNodeByID(ctx context.Context, session neo4j.SessionWithContext, id string) (neo4j.Node, []string, error) {
	nodeResult, err := session.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		query := `MATCH (n {id: $id}) RETURN n, labels(n) AS labels`
		result, err := tx.Run(ctx, query, map[string]any{"id": id})
		if err != nil {
			return nil, fmt.Errorf("DAL: 运行获取节点查询失败: %w", err)
		}
		record, err := result.Single(ctx)
		if err != nil {
			usageErr := new(neo4j.UsageError)
			if errors.As(err, &usageErr) && strings.Contains(usageErr.Error(), "result contains no more records") {
				return nil, nil // Indicate not found
			}
			return nil, fmt.Errorf("DAL: 获取节点结果失败: %w", err)
		}
		nodeInterface, _ := record.Get("n")
		labelsInterface, _ := record.Get("labels")
		dbNode, ok := nodeInterface.(dbtype.Node)
		if !ok {
			return nil, fmt.Errorf("DAL: 结果中的 'n' 不是有效的节点类型")
		}
		labelsRaw, ok := labelsInterface.([]any)
		if !ok {
			return nil, fmt.Errorf("DAL: 无法解析节点标签")
		}
		labels := make([]string, len(labelsRaw))
		for i, l := range labelsRaw {
			labels[i] = l.(string)
		}
		return map[string]any{"node": dbNode, "labels": labels}, nil
	})
	if err != nil {
		return dbtype.Node{}, nil, err
	}
	if nodeResult == nil { // 处理未找到的情况
		return dbtype.Node{}, nil, nil // 返回零值和 nil error 表示未找到
	}
	resultMap := nodeResult.(map[string]any)
	return resultMap["node"].(dbtype.Node), resultMap["labels"].([]string), nil
}

// ExecUpdateNode 执行更新节点的 Cypher
func (d *neo4jNodeDAL) ExecUpdateNode(ctx context.Context, session neo4j.SessionWithContext, id string, updates map[string]any) (neo4j.Node, []string, error) {
	// 注意：updates map 由 Repo 层准备好，包含要 SET 的属性
	nodeResult, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		// 构建 SET 子句
		setClauses := []string{}
		params := map[string]any{"id": id}
		for key, value := range updates {
			// 避免注入，使用参数
			paramName := "update_" + key
			setClauses = append(setClauses, fmt.Sprintf("n.%s = $%s", key, paramName))
			params[paramName] = value
		}
		// 总是更新 updated_at
		setClauses = append(setClauses, "n.updated_at = $now")
		params["now"] = time.Now().UTC()

		query := fmt.Sprintf(`MATCH (n {id: $id}) SET %s RETURN n, labels(n) AS labels`, strings.Join(setClauses, ", "))

		result, err := tx.Run(ctx, query, params)
		if err != nil {
			return nil, fmt.Errorf("DAL: 运行更新节点查询失败: %w", err)
		}
		record, err := result.Single(ctx)
		if err != nil {
			// 处理节点未找到等错误
			return nil, fmt.Errorf("DAL: 获取更新节点结果失败: %w", err)
		}
		nodeInterface, _ := record.Get("n")
		labelsInterface, _ := record.Get("labels")
		dbNode, ok := nodeInterface.(dbtype.Node)
		if !ok {
			return nil, fmt.Errorf("DAL: 结果中的 'n' 不是有效的节点类型")
		}
		labelsRaw, ok := labelsInterface.([]any)
		if !ok {
			return nil, fmt.Errorf("DAL: 无法解析节点标签")
		}
		labels := make([]string, len(labelsRaw))
		for i, l := range labelsRaw {
			labels[i] = l.(string)
		}
		return map[string]any{"node": dbNode, "labels": labels}, nil
	})
	if err != nil {
		return dbtype.Node{}, nil, err
	}
	resultMap := nodeResult.(map[string]any)
	return resultMap["node"].(dbtype.Node), resultMap["labels"].([]string), nil
}

// ExecDeleteNode 执行删除节点的 Cypher
func (d *neo4jNodeDAL) ExecDeleteNode(ctx context.Context, session neo4j.SessionWithContext, id string) error {
	_, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		// 使用 DETACH DELETE 删除节点及其所有关系
		query := `MATCH (n {id: $id}) DETACH DELETE n`
		_, err := tx.Run(ctx, query, map[string]any{"id": id})
		// 可以检查 ResultSummary().Counters().NodesDeleted() 来确认是否真的删除了
		// 如果节点不存在，Run 不会报错，所以这里简单返回错误
		return nil, err
	})
	if err != nil {
		return fmt.Errorf("DAL: 运行删除节点查询失败: %w", err)
	}
	return nil
}

// ExecSearchNodes 执行搜索节点的 Cypher
func (d *neo4jNodeDAL) ExecSearchNodes(ctx context.Context, session neo4j.SessionWithContext, keyword string, nodeType *network.NodeType, limit, offset int64) ([]neo4j.Node, [][]string, int64, error) {
	nodes := []neo4j.Node{}
	labelsList := [][]string{}
	var total int64 = 0

	readResult, err := session.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		var queryBuilder strings.Builder
		params := map[string]any{
			"keyword": ".*(?i)" + keyword + ".*", // 使用正则表达式进行模糊匹配 (大小写不敏感)
			"limit":   limit,
			"offset":  offset,
		}

		// 构建 MATCH 子句
		matchClause := "MATCH (n)"
		if nodeType != nil {
			matchClause = fmt.Sprintf("MATCH (n:%s)", nodeType.String())
		}
		queryBuilder.WriteString(matchClause)

		// 构建 WHERE 子句 (在 name 或其他需要搜索的属性上搜索)
		// 这里仅在 name 上搜索作为示例
		queryBuilder.WriteString(" WHERE n.name =~ $keyword")

		// --- 获取总数 ---
		countQuery := queryBuilder.String() + " RETURN count(n) AS total"
		countResult, err := tx.Run(ctx, countQuery, params)
		if err != nil {
			return nil, fmt.Errorf("DAL: 运行搜索节点总数查询失败: %w", err)
		}
		countRecord, err := countResult.Single(ctx)
		if err != nil {
			usageErr := new(neo4j.UsageError)
			if errors.As(err, &usageErr) && strings.Contains(usageErr.Error(), "result contains no more records") {
				total = 0
			} else {
				return nil, fmt.Errorf("DAL: 获取搜索节点总数失败: %w", err)
			}
		} else {
			totalVal, _ := countRecord.Get("total")
			total = totalVal.(int64)
		}

		if total == 0 {
			return map[string]any{"nodes": nodes, "labels": labelsList, "total": total}, nil // 如果总数为0，直接返回
		}

		// --- 获取节点数据 ---
		dataQuery := queryBuilder.String() + " RETURN n, labels(n) AS labels ORDER BY n.name SKIP $offset LIMIT $limit"
		dataResult, err := tx.Run(ctx, dataQuery, params)
		if err != nil {
			return nil, fmt.Errorf("DAL: 运行搜索节点数据查询失败: %w", err)
		}

		records, err := dataResult.Collect(ctx)
		if err != nil {
			return nil, fmt.Errorf("DAL: 收集搜索节点结果失败: %w", err)
		}

		nodes = make([]neo4j.Node, len(records))
		labelsList = make([][]string, len(records))
		for i, record := range records {
			nodeInterface, _ := record.Get("n")
			labelsInterface, _ := record.Get("labels")
			nodes[i] = nodeInterface.(dbtype.Node)
			labelsRaw := labelsInterface.([]any)
			labels := make([]string, len(labelsRaw))
			for j, l := range labelsRaw {
				labels[j] = l.(string)
			}
			labelsList[i] = labels
		}
		return map[string]any{"nodes": nodes, "labels": labelsList, "total": total}, nil
	})

	if err != nil {
		return nil, nil, 0, err
	}
	resultMap := readResult.(map[string]any)
	return resultMap["nodes"].([]neo4j.Node), resultMap["labels"].([][]string), resultMap["total"].(int64), nil
}

// ExecGetNetwork 执行网络查询的 Cypher
func (d *neo4jNodeDAL) ExecGetNetwork(ctx context.Context, session neo4j.SessionWithContext, profession string, depth int32) ([]neo4j.Node, []neo4j.Relationship, error) {
	// TODO: 实现 GetNetwork 的 Cypher 查询
	// 需要匹配特定职业的 Person 节点，然后根据 depth 扩展关系
	// 例如: MATCH path = (p:PERSON {profession: $profession})-[*1..$depth]-(neighbor)
	//       UNWIND nodes(path) as n
	//       UNWIND relationships(path) as r
	//       RETURN collect(distinct n) as nodes, collect(distinct r) as relations
	fmt.Printf("TODO: DAL 实现 ExecGetNetwork, profession: %s, depth: %d\n", profession, depth)
	return nil, nil, fmt.Errorf("DAL: ExecGetNetwork 未实现")
}

// ExecGetPath 执行路径查询的 Cypher
func (d *neo4jNodeDAL) ExecGetPath(ctx context.Context, session neo4j.SessionWithContext, sourceID, targetID string, maxDepth int32, relTypes []string) ([]neo4j.Node, []neo4j.Relationship, error) {
	// TODO: 实现 GetPath 的 Cypher 查询
	// 可能使用 allShortestPaths 或指定类型的变长匹配
	// 例如: MATCH path = allShortestPaths((source {id: $sourceId})-[r*1..$maxDepth]-(target {id: $targetId}))
	//       WHERE ALL(rel in relationships(path) WHERE type(rel) IN $relTypes OR size($relTypes)=0)
	//       RETURN nodes(path) as nodes, relationships(path) as relations LIMIT 1 // 通常只返回一条路径
	fmt.Printf("TODO: DAL 实现 ExecGetPath, source: %s, target: %s\n", sourceID, targetID)
	return nil, nil, fmt.Errorf("DAL: ExecGetPath 未实现")
}
