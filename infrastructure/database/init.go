package database

import (
	"context"
	"fmt"
	"log" // 建议使用更专业的日志库

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// InitNeo4j 初始化 Neo4j 驱动并应用 Schema
// 返回创建好的驱动实例，如果初始化失败则返回错误。
func InitNeo4j(config Neo4jConfig) (neo4j.DriverWithContext, error) {
	// 使用配置创建驱动
	driver, err := neo4j.NewDriverWithContext(config.URI, neo4j.BasicAuth(config.Username, config.Password, ""))
	if err != nil {
		return nil, fmt.Errorf("无法创建 Neo4j 驱动: %w", err)
	}

	// 检查连接性
	ctx := context.Background() // 或者使用带有超时的 context
	err = driver.VerifyConnectivity(ctx)
	if err != nil {
		driver.Close(ctx) // 关闭无效的驱动
		return nil, fmt.Errorf("无法连接到 Neo4j: %w", err)
	}
	log.Println("成功连接到 Neo4j")

	// 应用 Schema (索引和约束)
	err = applyNeo4jSchema(ctx, driver)
	if err != nil {
		// 通常不应因为 Schema 初始化失败而停止服务，记录警告即可
		log.Printf("警告: 应用 Neo4j Schema 失败: %v", err)
	} else {
		log.Println("成功应用 Neo4j Schema")
	}

	return driver, nil
}

// applyNeo4jSchema 创建必要的索引和约束
func applyNeo4jSchema(ctx context.Context, driver neo4j.DriverWithContext) error {
	session := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)

	// 定义要应用的 Schema 查询语句
	schemaQueries := []string{
		// Person 节点
		"CREATE CONSTRAINT unique_person_id IF NOT EXISTS FOR (p:PERSON) REQUIRE p.id IS UNIQUE;",
		"CREATE INDEX index_person_id IF NOT EXISTS FOR (p:PERSON) ON (p.id);",
		"CREATE INDEX index_person_profession IF NOT EXISTS FOR (p:PERSON) ON (p.profession);", // 为 GetNetwork 查询优化

		// Company 节点
		"CREATE CONSTRAINT unique_company_id IF NOT EXISTS FOR (c:COMPANY) REQUIRE c.id IS UNIQUE;",
		"CREATE INDEX index_company_id IF NOT EXISTS FOR (c:COMPANY) ON (c.id);",

		// School 节点
		"CREATE CONSTRAINT unique_school_id IF NOT EXISTS FOR (s:SCHOOL) REQUIRE s.id IS UNIQUE;",
		"CREATE INDEX index_school_id IF NOT EXISTS FOR (s:SCHOOL) ON (s.id);",

		// 可以考虑为关系ID添加唯一约束或索引，但这不太常见，除非你需要直接按关系ID查询
		// "CREATE CONSTRAINT unique_relation_id IF NOT EXISTS FOR ()-[r]-() REQUIRE r.id IS UNIQUE;",
	}

	// 在事务中执行每个 Schema 查询
	for _, query := range schemaQueries {
		_, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
			_, txErr := tx.Run(ctx, query, nil)
			// 如果索引/约束已存在，Run 会返回错误，但 ExecuteWrite 会处理好。
			// 这里我们简单地忽略内部错误，因为使用了 IF NOT EXISTS
			if txErr != nil {
				log.Printf("执行 Schema 查询 '%s' 时出错 (可能已存在): %v", query, txErr)
			}
			return nil, nil // 即使出错也继续尝试下一个
		})
		// ExecuteWrite 本身也可能出错（例如连接问题）
		if err != nil {
			// 如果事务本身失败，则停止并返回错误
			return fmt.Errorf("应用 schema '%s' 失败: %w", query, err)
		}
	}

	return nil
}
