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

		// 2. 构建更新查询
		var setClauses []string
		params := map[string]any{"id": id} // 初始化参数 map
		for key, value := range updates {
			paramName := "update_" + key
			setClauses = append(setClauses, fmt.Sprintf("n.%s = $%s", key, paramName))
			params[paramName] = value
		}

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

// ExecSearchNodes 执行搜索节点的 Cypher，返回匹配的节点、标签列表和总数。
func (d *neo4jNodeDAL) ExecSearchNodes(ctx context.Context, session neo4j.SessionWithContext, criteria map[string]string, nodeType *network.NodeType, limit, offset int64) ([]neo4j.Node, [][]string, int64, error) {
	// --- Remove Debug Logging --- VVV
	/*
		var nodeTypeStr string
		if nodeType != nil {
			nodeTypeStr = nodeType.String()
		} else {
			nodeTypeStr = "<nil>"
		}
		fmt.Printf("DEBUG DAL: ExecSearchNodes called with criteria=%v, nodeType=%s, limit=%d, offset=%d\n", criteria, nodeTypeStr, limit, offset)
	*/
	// --- End Remove Debug Logging ---

	// Base params map for the main query, including pagination
	mainParams := map[string]any{
		"limit":  limit,
		"offset": offset,
	}
	// Separate params map for the count query, initially empty or with only criteria params
	countParams := make(map[string]any)

	// --- Restore original MATCH logic --- VVV
	var matchClause string
	var whereClauses []string // Restore whereClauses

	// --- Restore original MATCH logic --- VVV
	if nodeType != nil {
		matchClause = fmt.Sprintf("MATCH (n:%s)", nodeType.String())
	} else {
		matchClause = "MATCH (n)"
	}
	// --- Remove DEBUG comments ---
	/*
		// --- DEBUG: Hardcode label for PERSON type --- VVV
		if nodeType != nil {
			if *nodeType == network.NodeType_PERSON {
				matchClause = "MATCH (n:PERSON)" // Hardcode PERSON label
			} else {
				matchClause = fmt.Sprintf("MATCH (n:%s)", nodeType.String())
			}
		} else {
			matchClause = "MATCH (n)"
		}
		// --- End DEBUG ---
	*/

	propIdx := 0
	for key, value := range criteria {
		// --- Revert: Use Parameterized query --- VVV
		paramName := fmt.Sprintf("prop_%d", propIdx)
		// --- 修改: 对 name 属性使用 CONTAINS --- VVV
		var clause string
		if key == "name" {
			clause = fmt.Sprintf("n.%s CONTAINS $%s", key, paramName)
		} else {
			// 对于其他属性，暂时还使用精确匹配 (可以根据需要扩展)
			clause = fmt.Sprintf("n.%s = $%s", key, paramName)
		}
		whereClauses = append(whereClauses, clause)
		// --- End 修改 --- ^^^
		// Add criteria params to BOTH maps
		mainParams[paramName] = value
		countParams[paramName] = value

		propIdx++
	}
	// --- End Revert --- ^^^

	// Build count query string
	countQueryBuilder := strings.Builder{}
	countQueryBuilder.WriteString(matchClause) // Use restored matchClause
	// Restore WHERE clause logic
	if len(whereClauses) > 0 {
		countQueryBuilder.WriteString(" WHERE ")
		countQueryBuilder.WriteString(strings.Join(whereClauses, " AND "))
	}
	countQueryBuilder.WriteString(" RETURN count(DISTINCT n) AS total")
	countQuery := countQueryBuilder.String()

	// Build main query string
	queryBuilder := strings.Builder{}
	queryBuilder.WriteString(matchClause) // Use restored matchClause
	// Restore WHERE clause logic
	if len(whereClauses) > 0 {
		queryBuilder.WriteString(" WHERE ")
		queryBuilder.WriteString(strings.Join(whereClauses, " AND "))
	}
	queryBuilder.WriteString(" RETURN DISTINCT n, labels(n) AS labels")
	queryBuilder.WriteString(" ORDER BY n.name") // Keep ordering
	queryBuilder.WriteString(" SKIP $offset LIMIT $limit")
	finalQuery := queryBuilder.String()

	var nodes []neo4j.Node
	var labelsList [][]string
	var total int64

	// Use ExecuteRead for both queries within the same transaction
	_, err := session.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		// Get total count first using countParams (no limit/offset)
		countResult, err := tx.Run(ctx, countQuery, countParams) // <<< Use countParams
		if err != nil {
			return nil, fmt.Errorf("DAL: running count query failed: %w", err)
		}
		countRecord, err := countResult.Single(ctx)
		if err != nil {
			if neo4jErr := new(neo4j.UsageError); errors.As(err, &neo4jErr) && strings.Contains(neo4jErr.Error(), "result contains no more records") {
				total = 0
			} else {
				return nil, fmt.Errorf("DAL: getting count result failed: %w", err)
			}
		} else {
			totalInterface, ok := countRecord.Get("total")
			if !ok {
				return nil, fmt.Errorf("DAL: count result missing 'total' field")
			}
			totalConv, ok := totalInterface.(int64)
			if !ok {
				return nil, fmt.Errorf("DAL: 'total' field is not int64")
			}
			total = totalConv // Assign converted value
		}

		// If total is 0, no need to run the main query
		if total == 0 {
			nodes = []neo4j.Node{}
			labelsList = [][]string{}
			return nil, nil // Indicate success with empty results
		}

		// Run the main query to get paginated nodes using mainParams (with limit/offset)
		// Restore using mainParams directly as parameter logic is reverted
		result, err := tx.Run(ctx, finalQuery, mainParams) // <<< Use mainParams
		if err != nil {
			return nil, fmt.Errorf("DAL: running main search query failed: %w", err)
		}

		// Collect results
		records, err := result.Collect(ctx)
		if err != nil {
			return nil, fmt.Errorf("DAL: collecting search results failed: %w", err)
		}

		nodes = make([]neo4j.Node, 0, len(records))
		labelsList = make([][]string, 0, len(records))
		for _, record := range records {
			nodeInterface, nodeOk := record.Get("n")
			labelsInterface, labelsOk := record.Get("labels")
			if !nodeOk || !labelsOk {
				fmt.Println("DAL: Warning - search result record missing 'n' or 'labels'") // TODO: Use logger
				continue
			}
			dbNode, nodeCastOk := nodeInterface.(dbtype.Node)
			labelsRaw, labelsCastOk := labelsInterface.([]any)
			if !nodeCastOk || !labelsCastOk {
				fmt.Println("DAL: Warning - search result record has incorrect type for 'n' or 'labels'") // TODO: Use logger
				continue
			}

			labels := make([]string, len(labelsRaw))
			validLabels := true
			for i, l := range labelsRaw {
				labelStr, ok := l.(string)
				if !ok {
					fmt.Printf("DAL: Warning - label is not a string: %v\n", l) // TODO: Use logger
					validLabels = false
					break
				}
				labels[i] = labelStr
			}
			if validLabels {
				nodes = append(nodes, dbNode)
				labelsList = append(labelsList, labels)
			}
		}

		return nil, nil // Transaction successful
	})

	if err != nil {
		return nil, nil, 0, err // Return error from transaction
	}

	// Return collected results
	return nodes, labelsList, total, nil
}

// ExecGetNetwork 执行网络查询的 Cypher。
// 根据起始节点条件、深度、关系类型和节点类型查询相关节点和关系。
func (d *neo4jNodeDAL) ExecGetNetwork(ctx context.Context, session neo4j.SessionWithContext,
	startNodeCriteria map[string]string,
	depth int32,
	limit, offset int64,
	relationTypes []network.RelationType,
	nodeTypes []network.NodeType,
) ([]neo4j.Node, []neo4j.Relationship, error) {

	// --- Handle Depth 0 Case --- (Added)
	if depth == 0 {
		return d.execGetNetworkDepthZero(ctx, session, startNodeCriteria, limit, offset, nodeTypes)
	}
	// --- End Handle Depth 0 Case ---

	// Existing logic for depth > 0
	var nodes []dbtype.Node
	var relationships []dbtype.Relationship

	// 确保深度至少为 1
	if depth <= 0 {
		depth = 1
	}

	readResult, err := session.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		var queryBuilder strings.Builder
		params := map[string]any{
			"offset": offset,
			"limit":  limit,
			// RelationTypes and NodeTypes will be handled in WHERE if needed
		}

		// Build MATCH clause for startNode based on criteria
		queryBuilder.WriteString("MATCH (startNode)")
		startWhereClauses := []string{}
		if len(startNodeCriteria) > 0 {
			i := 0
			for key, value := range startNodeCriteria {
				if key != "" && value != "" {
					paramName := fmt.Sprintf("start_%s", key)
					startWhereClauses = append(startWhereClauses, fmt.Sprintf("startNode.%s = $%s", key, paramName))
					params[paramName] = value
					i++
				}
			}
		}
		if len(startWhereClauses) > 0 {
			queryBuilder.WriteString(" WHERE ")
			queryBuilder.WriteString(strings.Join(startWhereClauses, " AND "))
		} else {
			// If no criteria, maybe return error or match any node? For now, let it match any.
			// This could be inefficient. Consider requiring start criteria.
		}

		// Build the path MATCH and WHERE clause for types
		queryBuilder.WriteString(fmt.Sprintf(" MATCH path = (startNode)-[*1..%d]-(neighbor)", depth))

		whereClauses := []string{}
		// Filter by relation types
		if len(relationTypes) > 0 {
			relTypeStrings := make([]string, len(relationTypes))
			for i, rt := range relationTypes {
				relTypeStrings[i] = rt.String()
			}
			whereClauses = append(whereClauses, "ALL(r IN relationships(path) WHERE type(r) IN $relTypes)")
			params["relTypes"] = relTypeStrings
		}

		// Filter by node types (apply to all nodes in the path, including start/end)
		if len(nodeTypes) > 0 {
			nodeTypeStrings := make([]string, len(nodeTypes))
			for i, nt := range nodeTypes {
				nodeTypeStrings[i] = nt.String()
			}
			whereClauses = append(whereClauses, "ALL(n IN nodes(path) WHERE ANY(lbl IN labels(n) WHERE lbl IN $nodeTypes))")
			params["nodeTypes"] = nodeTypeStrings
		}

		if len(whereClauses) > 0 {
			queryBuilder.WriteString(" WHERE ")
			queryBuilder.WriteString(strings.Join(whereClauses, " AND "))
		}

		// Final part of the query to collect and paginate
		queryBuilder.WriteString(`
			WITH path
			UNWIND nodes(path) as n
			UNWIND relationships(path) as r
			WITH collect(distinct n) as all_nodes, collect(distinct r) as all_rels
			RETURN
				CASE size(all_nodes) > $offset WHEN true THEN all_nodes[$offset..$offset+$limit] ELSE [] END AS nodes,
				CASE size(all_rels) > $offset WHEN true THEN all_rels[$offset..$offset+$limit] ELSE [] END AS relations
		`)

		query := queryBuilder.String()

		result, err := tx.Run(ctx, query, params)
		if err != nil {
			return nil, fmt.Errorf("DAL: 运行 GetNetwork 查询失败: %w", err)
		}

		record, err := result.Single(ctx)
		if err != nil {
			usageErr := new(neo4j.UsageError)
			if errors.As(err, &usageErr) && strings.Contains(usageErr.Error(), "result contains no more records") {
				// No matching path found based on criteria/types
				return map[string]any{"nodes": nodes, "rels": relationships}, nil
			}
			return nil, fmt.Errorf("DAL: 获取 GetNetwork 结果失败: %w", err)
		}

		// Extract node and relation lists (rest of the parsing logic remains similar)
		nodesInterface, nodesOk := record.Get("nodes")
		relsInterface, relsOk := record.Get("relations")
		if !nodesOk || !relsOk {
			return nil, fmt.Errorf("DAL: GetNetwork 查询返回结果缺少 'nodes' 或 'relations' 字段")
		}
		nodesRaw, ok := nodesInterface.([]any)
		if !ok {
			return nil, fmt.Errorf("DAL: 无法将 'nodes' 断言为 []any")
		}
		nodes = make([]dbtype.Node, len(nodesRaw))
		for i, nodeRaw := range nodesRaw {
			node, nodeAssertionOk := nodeRaw.(dbtype.Node)
			if !nodeAssertionOk {
				return nil, fmt.Errorf("DAL: 无法将 'nodes' 列表中的元素断言为 dbtype.Node")
			}
			nodes[i] = node
		}
		relsRaw, ok := relsInterface.([]any)
		if !ok {
			return nil, fmt.Errorf("DAL: 无法将 'relations' 断言为 []any")
		}
		relationships = make([]dbtype.Relationship, len(relsRaw))
		for i, relRaw := range relsRaw {
			rel, relAssertionOk := relRaw.(dbtype.Relationship)
			if !relAssertionOk {
				return nil, fmt.Errorf("DAL: 无法将 GetNetwork 'relations' 列表中的元素断言为 dbtype.Relationship")
			}
			relationships[i] = rel
		}

		return map[string]any{"nodes": nodes, "rels": relationships}, nil
	})

	if err != nil {
		return nil, nil, err
	}

	resultMap := readResult.(map[string]any)
	finalNodes := resultMap["nodes"].([]dbtype.Node)
	finalRels := resultMap["rels"].([]dbtype.Relationship)

	return finalNodes, finalRels, nil
}

// --- Added Helper for Depth 0 --- (New Function)
func (d *neo4jNodeDAL) execGetNetworkDepthZero(
	ctx context.Context, session neo4j.SessionWithContext,
	startNodeCriteria map[string]string,
	limit, offset int64,
	nodeTypes []network.NodeType,
) ([]neo4j.Node, []neo4j.Relationship, error) {

	var startNodeClauses []string
	params := map[string]any{}
	for key, value := range startNodeCriteria {
		paramName := "start_" + key
		startNodeClauses = append(startNodeClauses, fmt.Sprintf("startNode.%s = $%s", key, paramName))
		params[paramName] = value
	}

	// Build node type filter if provided
	nodeTypeFilter := ""
	if len(nodeTypes) > 0 {
		var typeLabels []string
		for _, nt := range nodeTypes {
			typeLabels = append(typeLabels, nt.String())
		}
		nodeTypeFilter = fmt.Sprintf("AND any(lbl IN labels(startNode) WHERE lbl IN ['%s'])", strings.Join(typeLabels, "', '"))
	}

	// Build the simplified query for depth 0
	query := fmt.Sprintf(`
        MATCH (startNode)
        WHERE %s %s
        RETURN startNode
        ORDER BY startNode.id // Consistent ordering is good practice
        SKIP $offset
        LIMIT $limit
    `, strings.Join(startNodeClauses, " AND "), nodeTypeFilter)

	params["offset"] = offset
	params["limit"] = limit

	var nodes []dbtype.Node
	_, err := session.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		result, err := tx.Run(ctx, query, params)
		if err != nil {
			return nil, fmt.Errorf("DAL: 运行 GetNetwork(Depth 0) 查询失败: %w", err)
		}
		records, err := result.Collect(ctx)
		if err != nil {
			return nil, fmt.Errorf("DAL: 收集 GetNetwork(Depth 0) 结果失败: %w", err)
		}
		for _, record := range records {
			nodeInterface, _ := record.Get("startNode")
			if dbNode, ok := nodeInterface.(dbtype.Node); ok {
				nodes = append(nodes, dbNode)
			} else {
				fmt.Printf("WARN: GetNetwork(Depth 0) 结果中的 'startNode' 类型断言失败\n")
			}
		}
		return nil, nil
	})

	if err != nil {
		return nil, nil, err
	}

	// For depth 0, relations are always empty
	return nodes, []dbtype.Relationship{}, nil
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
