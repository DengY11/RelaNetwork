package neo4jdal

import (
	"context"
	"fmt"
	"strings"

	network "labelwall/biz/model/relationship/network"

	"errors"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/dbtype"
)

// TODO: 把错误信息改为日志系统
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

	// 在事务函数外部声明需要返回的变量
	var updatedNode dbtype.Node
	var labels []string

	// 使用 ExecuteWrite 在事务中执行写操作
	_, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		// 1. 检查节点是否存在
		// 现在已改为调用 ExecUpdateNode 之前由 Repo 层检查，或者允许更新操作本身在节点不存在时静默失败或由 Cypher 处理
		/* // 暂时注释掉内部的存在性检查，依赖 Cypher 的 MATCH 行为
		existsQuery := "MATCH (n{id: $id}) RETURN count(n) AS cnt"
		existsResult, err := tx.Run(ctx, existsQuery, map[string]any{"id": id})
		if err != nil {
			return nil, fmt.Errorf("DAL: 检查节点存在性失败: %w", err)
		}
		recordExists, err := existsResult.Single(ctx)
		if err != nil {
			return nil, fmt.Errorf("DAL: 获取节点存在性结果失败: %w", err)
		}
		count, _ := recordExists.Get("count")
		if count.(int64) == 0 {
			// 如果节点不存在，Neo4j 的 MATCH..SET..RETURN 会失败，错误会由后续的 result.Single() 捕获
			// return nil, fmt.Errorf("DAL: 节点ID %s 不存在", id) // 返回特定错误可能更好
		}
		*/

		// 2. 构建更新查询
		var setClauses []string
		params := map[string]any{"id": id} // 初始化参数 map
		for key, value := range updates {
			paramName := "update_" + key
			setClauses = append(setClauses, fmt.Sprintf("n.%s = $%s", key, paramName))
			params[paramName] = value
		}
		// updated_at 已由 repo 层传入 updates map，无需在此处重复添加
		// setClauses = append(setClauses, "n.updated_at = $now")
		// params["now"] = time.Now().UTC()

		// 如果没有要更新的属性（例如只更新 updated_at），setClauses 会为空，需要处理
		if len(setClauses) == 0 {
			// 也许只更新时间戳？或者返回错误？取决于业务逻辑
			// 如果 repo 层总是确保有 updated_at，这里至少会有一条
			// return nil, fmt.Errorf("DAL: 没有提供要更新的属性")
			// 暂定：如果传入的 updates 为空，则可能只更新时间戳（如果业务逻辑如此）
			// 假设 repo 层会处理 updates map 的内容
			return nil, fmt.Errorf("DAL: 内部错误 - 更新属性列表为空") // 或者采取其他策略
		}

		query := fmt.Sprintf(`MATCH (n {id: $id}) SET %s RETURN n, labels(n) AS labels`, strings.Join(setClauses, ", "))

		result, err := tx.Run(ctx, query, params)
		if err != nil {
			return nil, fmt.Errorf("DAL: 运行更新节点查询失败: %w", err)
		}

		// 3. 获取更新后的结果
		record, err := result.Single(ctx)
		if err != nil {
			// 检查是否因为节点未找到而失败
			usageErr := new(neo4j.UsageError)
			if errors.As(err, &usageErr) && strings.Contains(usageErr.Error(), "result contains no more records") {
				// 返回表示未找到的错误，让 Repo 层处理
				return nil, fmt.Errorf("DAL: 未找到要更新的节点 ID %s", id)
			}
			return nil, fmt.Errorf("DAL: 获取更新节点结果失败: %w", err)
		}

		// 4. 解析节点和标签 (赋值给外部变量)
		nodeInterface, _ := record.Get("n")
		labelsInterface, _ := record.Get("labels")

		nodeVal, ok := nodeInterface.(dbtype.Node)
		if !ok {
			return nil, fmt.Errorf("DAL: 结果中的 'n' 不是有效的节点类型")
		}
		updatedNode = nodeVal // 赋值给外部变量

		labelsRaw, ok := labelsInterface.([]any)
		if !ok {
			return nil, fmt.Errorf("DAL: 无法解析节点标签")
		}
		lb := make([]string, len(labelsRaw))
		for i, l := range labelsRaw {
			labelStr, ok := l.(string)
			if !ok {
				return nil, fmt.Errorf("DAL: 无法将标签断言为字符串")
			}
			lb[i] = labelStr
		}
		labels = lb // 赋值给外部变量

		// 5. 消费结果并检查计数器 (确保 Consume 在 Single 之后)
		summary, summaryErr := result.Consume(ctx) // summary 变量在此作用域内
		if summaryErr != nil {
			// 即使 Single 成功，Consume 也可能出错
			return nil, fmt.Errorf("DAL: 消费更新结果失败: %w", summaryErr)
		}

		// 6. 添加对 summary 和 Counters() 的 nil 检查
		if summary == nil || summary.Counters() == nil {
			fmt.Printf("警告: 更新节点后 summary 或 counters 为 nil (ID: %s)\n", id) // TODO: 使用日志库
		} else if summary.Counters().PropertiesSet() == 0 {
			// 理论上如果 Single 成功，PropertiesSet 应该 > 0
			fmt.Printf("警告: 更新节点后 PropertiesSet 为 0 (ID: %s)\n", id) // TODO: 使用日志库
		}

		return nil, nil // 事务成功
	})

	if err != nil {
		// 返回事务执行中遇到的错误 (包括自定义的 EntityNotFound)
		return dbtype.Node{}, nil, err
	}

	// 如果事务成功，返回从事务内部赋值的节点和标签
	return updatedNode, labels, nil
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

// ExecSearchNodes 执行搜索节点的 Cypher，支持按类型和多个属性模糊搜索，并进行分页。
// criteria map 的 key 是属性名，value 是要搜索的字符串值。
func (d *neo4jNodeDAL) ExecSearchNodes(ctx context.Context, session neo4j.SessionWithContext, criteria map[string]string, nodeType *network.NodeType, limit, offset int64) ([]neo4j.Node, [][]string, int64, error) {

	// return value
	var (
		nodes      []neo4j.Node
		labelsList [][]string
		total      int64 = 0
	)

	readResult, err := session.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		var queryBuilder strings.Builder // 用于构建查询语句
		params := map[string]any{        // 初始化查询参数
			"limit":  limit,
			"offset": offset,
		}

		// 构建 MATCH 子句，如果指定了节点类型，则添加标签过滤。
		matchClause := "MATCH (n)"
		if nodeType != nil {
			matchClause = fmt.Sprintf("MATCH (n:%s)", nodeType.String())
		}
		queryBuilder.WriteString(matchClause)

		// 构建 WHERE 子句
		whereClauses := []string{}
		if len(criteria) > 0 {
			queryBuilder.WriteString(" WHERE ")
			i := 0
			for key, value := range criteria {
				// 简单处理：假设所有传入的 criteria key 都是有效的节点属性名
				// 并且所有 value 都需要进行模糊匹配
				if key != "" && value != "" { // 忽略空的键或值
					paramName := fmt.Sprintf("criteria_%d", i)
					// 使用 =~ 进行大小写不敏感的正则匹配
					// 注意：确保属性名 key 是安全的，不允许用户直接输入 key 以防注入 Cypher 片段
					// Repo 层应负责验证传入的 criteria key 是合法的属性名 (如 name, profession)
					whereClauses = append(whereClauses, fmt.Sprintf("n.%s =~ $%s", key, paramName))
					params[paramName] = ".*(?i)" + value + ".*" // 构造正则表达式
					i++
				}
			}
		}
		if len(whereClauses) > 0 {
			queryBuilder.WriteString(strings.Join(whereClauses, " AND "))
		} else {
			// 如果没有提供任何搜索条件，为了避免匹配所有节点，可以返回错误或默认行为
			// 这里我们选择返回空结果，因为没有指定任何搜索属性
			return map[string]any{"nodes": nodes, "labels": labelsList, "total": total}, nil
		}

		// --- 第一步：获取匹配的总数（用于分页）---
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
			return map[string]any{"nodes": nodes, "labels": labelsList, "total": total}, nil
		}

		// --- 第二步：获取分页后的节点数据 ---
		// 添加排序，例如按名称排序
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

// ExecGetNetwork 执行网络查询的 Cypher。
// 根据职业和深度查询相关节点和关系。
// TODO：此查询可能返回大量数据，建议在调用层或 API 设计中加入限制。
// TODO: config文件应该包含depth配置
func (d *neo4jNodeDAL) ExecGetNetwork(ctx context.Context, session neo4j.SessionWithContext, profession string, depth int32, limit, offset int64) ([]neo4j.Node, []neo4j.Relationship, error) {
	var nodes []neo4j.Node
	var relationships []neo4j.Relationship

	// 确保深度至少为 1
	if depth <= 0 {
		depth = 1
	}

	readResult, err := session.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		// REMOVE unused declarations inside the transaction function
		/*
			// 使用 map 存储节点和关系，确保唯一性
			var nodes = make([]neo4j.Node, 0)
			var relationships = make([]neo4j.Relationship, 0)
		*/

		// Dynamically build the initial MATCH clause based on profession
		var matchClause string
		params := map[string]any{
			// Parameters needed regardless of the match clause
			"offset": offset,
			"limit":  limit,
		}

		if profession != "" {
			// If profession is provided, match PERSON nodes with that profession
			matchClause = "MATCH path = (startNode:PERSON {profession: $profession})-[*1..%d]-(neighbor)"
			params["profession"] = profession
		} else {
			// If profession is empty, match any node as startNode
			matchClause = "MATCH path = (startNode)-[*1..%d]-(neighbor)"
		}

		// Combine clauses
		// Warning: UNWIND and collect before slicing can be memory intensive for large graphs!
		query := fmt.Sprintf(`
			%s
			WITH path
			UNWIND nodes(path) as n
			UNWIND relationships(path) as r
			WITH collect(distinct n) as all_nodes, collect(distinct r) as all_rels
			RETURN
				CASE size(all_nodes) > $offset WHEN true THEN all_nodes[$offset..$offset+$limit] ELSE [] END AS nodes,
				CASE size(all_rels) > $offset WHEN true THEN all_rels[$offset..$offset+$limit] ELSE [] END AS relations
		`, fmt.Sprintf(matchClause, depth)) // Apply depth format to the chosen matchClause

		result, err := tx.Run(ctx, query, params)
		if err != nil {
			return nil, fmt.Errorf("DAL: 运行 GetNetwork 查询失败: %w", err)
		}

		record, err := result.Single(ctx) // 期望返回一行包含 nodes 和 relations 列表
		if err != nil {
			// 检查是否因为没有找到起始节点而无结果
			usageErr := new(neo4j.UsageError)
			if errors.As(err, &usageErr) && strings.Contains(usageErr.Error(), "result contains no more records") {
				// 没有找到匹配职业的节点，返回空结果
				return map[string]any{"nodes": nodes, "rels": relationships}, nil
			}
			return nil, fmt.Errorf("DAL: 获取 GetNetwork 结果失败: %w", err)
		}

		// 提取节点列表
		nodesInterface, nodesOk := record.Get("nodes")
		// 提取关系列表
		relsInterface, relsOk := record.Get("relations")

		if !nodesOk || !relsOk {
			return nil, fmt.Errorf("DAL: GetNetwork 查询返回结果缺少 'nodes' 或 'relations' 字段")
		}

		// 类型断言和转换节点列表
		nodesRaw, ok := nodesInterface.([]any)
		if !ok {
			return nil, fmt.Errorf("DAL: 无法将 'nodes' 断言为 []any")
		}
		nodes = make([]neo4j.Node, len(nodesRaw))
		for i, nodeRaw := range nodesRaw {
			node, nodeAssertionOk := nodeRaw.(dbtype.Node)
			if !nodeAssertionOk {
				return nil, fmt.Errorf("DAL: 无法将 'nodes' 列表中的元素断言为 dbtype.Node")
			}
			nodes[i] = node
		}

		// 类型断言和转换关系列表
		relsRaw, ok := relsInterface.([]any)
		if !ok {
			return nil, fmt.Errorf("DAL: 无法将 'relations' 断言为 []any")
		}
		relationships = make([]neo4j.Relationship, len(relsRaw))
		for i, relRaw := range relsRaw {
			rel, relAssertionOk := relRaw.(dbtype.Relationship)
			if !relAssertionOk {
				return nil, fmt.Errorf("DAL: 无法将 GetNetwork 'relations' 列表中的元素断言为 dbtype.Relationship")
			}
			relationships[i] = rel
		}

		// Return the results in a map for the outer function to process
		return map[string]any{"nodes": nodes, "rels": relationships}, nil // Use the locally parsed 'nodes' and 'relationships'
	})

	if err != nil {
		return nil, nil, err // 返回事务错误
	}

	// 解析事务返回的 map
	resultMap := readResult.(map[string]any)
	finalNodes := resultMap["nodes"].([]neo4j.Node)
	finalRels := resultMap["rels"].([]neo4j.Relationship)

	return finalNodes, finalRels, nil
}

// ExecGetPath 执行路径查询的 Cypher。
// 查找两个节点之间的路径，可指定最大深度和关系类型。
// TODO: config 文件应该包含depth设置
func (d *neo4jNodeDAL) ExecGetPath(ctx context.Context, session neo4j.SessionWithContext, sourceID, targetID string, maxDepth int32, relTypes []string) ([]neo4j.Node, []neo4j.Relationship, error) {
	var nodes []neo4j.Node
	var relationships []neo4j.Relationship

	// 确保最大深度至少为 1
	if maxDepth <= 0 {
		maxDepth = 1 // 或者根据业务逻辑返回错误
	}

	readResult, err := session.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		// 构建查询语句
		var queryBuilder strings.Builder
		queryBuilder.WriteString(fmt.Sprintf(`
            MATCH (source {id: $sourceId}), (target {id: $targetId})
            MATCH path = shortestPath((source)-[r*1..%d]-(target))
        `, maxDepth))

		params := map[string]any{
			"sourceId": sourceID,
			"targetId": targetID,
			"relTypes": relTypes, // 将 relTypes 列表作为参数传递
			// maxDepth 已直接嵌入查询字符串
		}

		// 如果指定了关系类型，则添加 WHERE 子句进行过滤
		if len(relTypes) > 0 {
			queryBuilder.WriteString(` WHERE ALL(rel IN relationships(path) WHERE type(rel) IN $relTypes)`) // $relTypes 参数是一个列表
		}

		queryBuilder.WriteString(` RETURN nodes(path) as nodes, relationships(path) as relations LIMIT 1`) // 即使 allShortestPaths 也只取一条

		query := queryBuilder.String()

		result, err := tx.Run(ctx, query, params)
		if err != nil {
			return nil, fmt.Errorf("DAL: 运行 GetPath 查询失败: %w", err)
		}

		record, err := result.Single(ctx) // shortestPath 只会返回一条或零条路径
		if err != nil {
			// 检查是否因为未找到路径而无结果
			usageErr := new(neo4j.UsageError)
			if errors.As(err, &usageErr) && strings.Contains(usageErr.Error(), "result contains no more records") {
				// 没有找到路径，返回空结果 (nil slices, nil error)
				return nil, nil
			}
			// 其他获取结果的错误
			return nil, fmt.Errorf("DAL: 获取 GetPath 结果失败: %w", err)
		}

		// 提取节点列表
		nodesInterface, nodesOk := record.Get("nodes")
		// 提取关系列表
		relsInterface, relsOk := record.Get("relations")

		if !nodesOk || !relsOk {
			return nil, fmt.Errorf("DAL: GetPath 查询返回结果缺少 'nodes' 或 'relations' 字段")
		}

		// 类型断言和转换节点列表
		nodesRaw, ok := nodesInterface.([]any)
		if !ok {
			return nil, fmt.Errorf("DAL: 无法将 'nodes' 断言为 []any")
		}
		nodes = make([]neo4j.Node, len(nodesRaw))
		for i, nodeRaw := range nodesRaw {
			node, nodeAssertionOk := nodeRaw.(dbtype.Node)
			if !nodeAssertionOk {
				return nil, fmt.Errorf("DAL: 无法将 GetPath 'nodes' 列表中的元素断言为 dbtype.Node")
			}
			nodes[i] = node
		}

		// 类型断言和转换关系列表
		relsRaw, ok := relsInterface.([]any)
		if !ok {
			return nil, fmt.Errorf("DAL: 无法将 'relations' 断言为 []any")
		}
		relationships = make([]neo4j.Relationship, len(relsRaw))
		for i, relRaw := range relsRaw {
			rel, relAssertionOk := relRaw.(dbtype.Relationship)
			if !relAssertionOk {
				return nil, fmt.Errorf("DAL: 无法将 GetPath 'relations' 列表中的元素断言为 dbtype.Relationship")
			}
			relationships[i] = rel
		}

		// 返回包含节点和关系的 map，供事务外解析
		return map[string]any{"nodes": nodes, "rels": relationships}, nil
	})

	if err != nil {
		return nil, nil, err // 返回事务错误
	}

	// 处理未找到路径的情况 (事务函数返回了 nil, nil)
	if readResult == nil {
		return nil, nil, nil // 返回 nil slices 和 nil error 表示未找到路径
	}

	// 解析事务返回的 map
	resultMap := readResult.(map[string]any)
	finalNodes := resultMap["nodes"].([]neo4j.Node)
	finalRels := resultMap["rels"].([]neo4j.Relationship)

	return finalNodes, finalRels, nil
}
