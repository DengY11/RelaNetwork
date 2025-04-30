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

// TODO: 把错误信息改为日志系统
// TODO: 完成execGetNetwork方法
// TODO: 完成execGetPath方法
// neo4jNodeDAL 实现了 NodeDAL 接口，封装了与节点相关的底层数据库操作。
type neo4jNodeDAL struct {
	// DAL 层通常不直接持有 driver，而是通过方法参数接收 session
}

// NewNodeDAL 创建一个新的 NodeDAL 实例。
// 返回 NodeDAL 接口类型，供repo层使用。
func NewNodeDAL() NodeDAL { // 返回接口类型
	return &neo4jNodeDAL{}
}

// ExecCreateNode 执行创建节点的 Cypher 语句。
// properties map 由 Repo 层构建，包含 id 和所有节点属性。
func (d *neo4jNodeDAL) ExecCreateNode(ctx context.Context, session neo4j.SessionWithContext, nodeType network.NodeType, properties map[string]any) (neo4j.Node, error) {
	// 使用 ExecuteWrite 在事务中执行写操作。
	nodeResult, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		// 构建 Cypher 查询语句，使用 NodeType 作为标签。
		query := fmt.Sprintf(`CREATE (n:%s $props) RETURN n`, nodeType.String())
		// 执行查询，传入属性 map。
		result, err := tx.Run(ctx, query, map[string]any{"props": properties})
		if err != nil {
			return nil, fmt.Errorf("DAL: 运行创建节点查询失败: %w", err)
		}
		// 获取单个结果记录。
		record, err := result.Single(ctx)
		// 这里用Single的原因: Single会检验结果中是只包含单个结果
		// 如果包含多个结果，会出现错误
		if err != nil {
			return nil, fmt.Errorf("DAL: 获取创建节点结果失败: %w", err)
		}
		// 从记录中提取节点数据。
		nodeInterface, ok := record.Get("n")
		if !ok {
			return nil, fmt.Errorf("DAL: 无法从结果中获取节点 'n'")
		}
		// 类型断言为 Neo4j 节点类型。
		dbNode, ok := nodeInterface.(dbtype.Node)
		if !ok {
			return nil, fmt.Errorf("DAL: 结果中的 'n' 不是有效的节点类型")
		}
		// 直接返回 Neo4j 驱动的节点类型。
		return dbNode, nil
	})

	// 如果事务发生错误返回零值节点和错误。
	if err != nil {
		return dbtype.Node{}, err
	}

	// 类型断言事务返回的结果。
	createdNode, ok := nodeResult.(dbtype.Node)
	if !ok {
		return dbtype.Node{}, fmt.Errorf("DAL: 事务返回了非预期的节点类型")
	}
	return createdNode, nil
}

// ExecGetNodeByID 执行按 ID 获取节点的 Cypher。
// 返回 Neo4j 节点、节点的标签列表以及错误。
func (d *neo4jNodeDAL) ExecGetNodeByID(ctx context.Context, session neo4j.SessionWithContext, id string) (neo4j.Node, []string, error) {
	// 使用 ExecuteRead 在事务中执行读操作。
	nodeResult, err := session.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		// 查询节点及其标签。
		query := `MATCH (n {id: $id}) RETURN n, labels(n) AS labels`
		result, err := tx.Run(ctx, query, map[string]any{"id": id})
		if err != nil {
			return nil, fmt.Errorf("DAL: 运行获取节点查询失败: %w", err)
		}

		record, err := result.Single(ctx)
		if err != nil {
			// 检查是否为"未找到记录"的特定错误。
			usageErr := new(neo4j.UsageError)
			if errors.As(err, &usageErr) && strings.Contains(usageErr.Error(), "result contains no more records") {
				return nil, nil // 返回 nil, nil 表示未找到，由 Repo 层处理。
			}
			// 其他获取结果的错误。
			return nil, fmt.Errorf("DAL: 获取节点结果失败: %w", err)
		}
		// 提取节点和标签数据。
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
		// 将标签列表转换为 []string。
		labels := make([]string, len(labelsRaw))
		for i, l := range labelsRaw {
			labels[i] = l.(string)
		}
		// 返回包含节点和标签的 map。
		return map[string]any{"node": dbNode, "labels": labels}, nil
	})
	// 处理事务执行错误。
	if err != nil {
		return dbtype.Node{}, nil, err
	}
	// 处理"未找到"的情况 (事务函数返回了 nil, nil)。
	if nodeResult == nil {
		return dbtype.Node{}, nil, nil // 返回零值节点、nil 标签和 nil 错误表示未找到。
	}
	// 解析事务返回的 map。
	resultMap := nodeResult.(map[string]any)
	return resultMap["node"].(dbtype.Node), resultMap["labels"].([]string), nil
}

// ExecUpdateNode 执行更新节点的 Cypher。
// updates map 由 Repo 层准备，包含需要 SET 的属性。
func (d *neo4jNodeDAL) ExecUpdateNode(ctx context.Context, session neo4j.SessionWithContext, id string, updates map[string]any) (neo4j.Node, []string, error) {
	// 使用 ExecuteWrite 执行写操作。
	nodeResult, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		// 动态构建 SET 子句。
		var setClauses []string

		params := map[string]any{"id": id} // 初始化参数 map
		for key, value := range updates {
			paramName := "update_" + key // 为每个更新值创建唯一的参数名
			setClauses = append(setClauses, fmt.Sprintf("n.%s = $%s", key, paramName))
			params[paramName] = value
		}
		// 总是自动更新 updated_at 时间戳。
		setClauses = append(setClauses, "n.updated_at = $now")
		params["now"] = time.Now().UTC()

		// 构建完整的 Cypher 更新语句。
		query := fmt.Sprintf(`MATCH (n {id: $id}) SET %s RETURN n, labels(n) AS labels`, strings.Join(setClauses, ", "))

		// 执行查询。
		result, err := tx.Run(ctx, query, params)
		if err != nil {
			return nil, fmt.Errorf("DAL: 运行更新节点查询失败: %w", err)
		}
		// 获取更新后的单个节点结果。
		record, err := result.Single(ctx)
		if err != nil {
			// 此处错误可能表示节点未找到。
			return nil, fmt.Errorf("DAL: 获取更新节点结果失败: %w", err)
		}
		// 解析返回的节点和标签。
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
	// 处理事务错误。
	if err != nil {
		return dbtype.Node{}, nil, err
	}
	// 解析结果 map。
	resultMap := nodeResult.(map[string]any)
	return resultMap["node"].(dbtype.Node), resultMap["labels"].([]string), nil
}

// ExecDeleteNode 执行根据id删除节点的 Cypher，并确认节点是否被删除。
// 如果节点不存在，则返回一个表示未找到的错误。
func (d *neo4jNodeDAL) ExecDeleteNode(ctx context.Context, session neo4j.SessionWithContext, id string) error {
	// 执行写事务。
	resultSummary, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		// 使用 DETACH DELETE 删除节点及其所有关联关系。
		query := `MATCH (n {id: $id}) DETACH DELETE n`
		result, err := tx.Run(ctx, query, map[string]any{"id": id})
		if err != nil {
			// 处理查询执行错误。
			return nil, fmt.Errorf("query execution failed: %w", err)
		}
		// 获取结果摘要信息。
		// result.Consume(ctx) 会返回错误如果 tx.Run 发生了错误。
		// result.Consume(ctx) 不会返回错误如果 MATCH 语句没有找到要删除的节点。
		summary, err := result.Consume(ctx)
		if err != nil {
			// 处理获取摘要时的错误。
			return nil, fmt.Errorf("result consumption failed: %w", err)
		}
		// 返回摘要信息供事务外检查。
		return summary, nil
	})

	// 处理事务本身的错误或事务函数返回的错误。
	if err != nil {
		return fmt.Errorf("DAL: 删除节点事务失败: %w", err)
	}

	// 事务成功后，检查结果摘要。
	if summary, ok := resultSummary.(neo4j.ResultSummary); ok {
		// 检查计数器中被删除的节点数量。
		if summary.Counters().NodesDeleted() == 0 {
			// 如果没有节点被删除，说明具有该 ID 的节点不存在。
			// 返回一个特定的错误（或哨兵错误）。
			return fmt.Errorf("DAL: node with id '%s' not found for deletion", id)
		}
		// 节点删除成功。
		return nil
	}

	// 如果 ExecuteWrite 成功但返回的不是预期的 ResultSummary 类型（理论上不太可能）。
	return fmt.Errorf("DAL: 删除节点事务返回了非预期的结果类型")
}

// ExecSearchNodes 执行搜索节点的 Cypher，支持按类型和关键词模糊搜索，并进行分页。
func (d *neo4jNodeDAL) ExecSearchNodes(ctx context.Context, session neo4j.SessionWithContext, keyword string, nodeType *network.NodeType, limit, offset int64) ([]neo4j.Node, [][]string, int64, error) {
	// 初始化返回值。
	nodes := []neo4j.Node{}
	labelsList := [][]string{}
	var total int64 = 0

	// 执行读事务。
	readResult, err := session.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		var queryBuilder strings.Builder // 用于构建查询语句
		params := map[string]any{ // 初始化查询参数
			"keyword": ".*(?i)" + keyword + ".*", // 正则表达式模糊匹配（忽略大小写）
			"limit":   limit,
			"offset":  offset,
		}

		// 构建 MATCH 子句，如果指定了节点类型，则添加标签过滤。
		matchClause := "MATCH (n)"
		if nodeType != nil {
			matchClause = fmt.Sprintf("MATCH (n:%s)", nodeType.String())
		}
		queryBuilder.WriteString(matchClause)

		// 构建 WHERE 子句，这里示例仅按名称搜索。
		// 可根据需要扩展到其他字段（如 profession）。
		queryBuilder.WriteString(" WHERE n.name =~ $keyword")

		// --- 第一步：获取匹配的总数（用于分页）---
		countQuery := queryBuilder.String() + " RETURN count(n) AS total"
		countResult, err := tx.Run(ctx, countQuery, params)
		if err != nil {
			return nil, fmt.Errorf("DAL: 运行搜索节点总数查询失败: %w", err)
		}
		// 获取总数结果。
		countRecord, err := countResult.Single(ctx)
		if err != nil {
			// 处理没有匹配项的情况（Single 会报错）。
			usageErr := new(neo4j.UsageError)
			if errors.As(err, &usageErr) && strings.Contains(usageErr.Error(), "result contains no more records") {
				total = 0 // 没有记录，总数为 0
			} else {
				return nil, fmt.Errorf("DAL: 获取搜索节点总数失败: %w", err)
			}
		} else {
			// 成功获取总数。
			totalVal, _ := countRecord.Get("total")
			total = totalVal.(int64)
		}

		// 如果总数为 0，无需查询数据，直接返回空结果和总数。
		if total == 0 {
			return map[string]any{"nodes": nodes, "labels": labelsList, "total": total}, nil
		}

		// --- 第二步：获取分页后的节点数据 ---
		dataQuery := queryBuilder.String() + " RETURN n, labels(n) AS labels ORDER BY n.name SKIP $offset LIMIT $limit" // 添加排序和分页
		dataResult, err := tx.Run(ctx, dataQuery, params)
		if err != nil {
			return nil, fmt.Errorf("DAL: 运行搜索节点数据查询失败: %w", err)
		}

		// 收集所有结果记录。
		records, err := dataResult.Collect(ctx)
		if err != nil {
			return nil, fmt.Errorf("DAL: 收集搜索节点结果失败: %w", err)
		}

		// 遍历记录，提取节点和标签信息。
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
		// 返回包含节点、标签列表和总数的 map。
		return map[string]any{"nodes": nodes, "labels": labelsList, "total": total}, nil
	})

	// 处理事务错误。
	if err != nil {
		return nil, nil, 0, err
	}
	// 解析事务返回的 map。
	resultMap := readResult.(map[string]any)
	return resultMap["nodes"].([]neo4j.Node), resultMap["labels"].([][]string), resultMap["total"].(int64), nil
}

// ExecGetNetwork 执行网络查询的 Cypher。
// 根据职业和深度查询相关节点和关系。
func (d *neo4jNodeDAL) ExecGetNetwork(ctx context.Context, session neo4j.SessionWithContext, profession string, depth int32) ([]neo4j.Node, []neo4j.Relationship, error) {
	// TODO: 实现 GetNetwork 的 Cypher 查询
	// TODO: 节点数量需要限制，以面被单个用户占用太多服务器性能
	// 需要匹配特定职业的 Person 节点，然后根据 depth 扩展关系。
	// 例如: MATCH path = (p:PERSON {profession: $profession})-[*1..$depth]-(neighbor)
	//       UNWIND nodes(path) as n
	//       UNWIND relationships(path) as r
	//       RETURN collect(distinct n) as nodes, collect(distinct r) as relations
	fmt.Printf("TODO: DAL 实现 ExecGetNetwork, profession: %s, depth: %d\n", profession, depth)
	return nil, nil, fmt.Errorf("DAL: ExecGetNetwork 未实现") // 占位符
}

// ExecGetPath 执行路径查询的 Cypher。
// 查找两个节点之间的路径，可指定最大深度和关系类型。
func (d *neo4jNodeDAL) ExecGetPath(ctx context.Context, session neo4j.SessionWithContext, sourceID, targetID string, maxDepth int32, relTypes []string) ([]neo4j.Node, []neo4j.Relationship, error) {
	// TODO: 实现 GetPath 的 Cypher 查询
	// TODO: 节点数量需要限制，以面被单个用户占用太多服务器性能
	// 可能使用 allShortestPaths 或指定类型的变长匹配。
	// 例如: MATCH path = allShortestPaths((source {id: $sourceId})-[r*1..$maxDepth]-(target {id: $targetId}))
	//       WHERE ALL(rel in relationships(path) WHERE type(rel) IN $relTypes OR size($relTypes)=0)
	//       RETURN nodes(path) as nodes, relationships(path) as relations LIMIT 1 // 通常只返回一条路径
	fmt.Printf("TODO: DAL 实现 ExecGetPath, source: %s, target: %s\n", sourceID, targetID)
	return nil, nil, fmt.Errorf("DAL: ExecGetPath 未实现") // 占位符
}
