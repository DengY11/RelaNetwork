package database

//TODO: 使用更专业的日志库
//TODO: 从配置文件中读取redis和neo4j的配置

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/redis/go-redis/v9" // 添加 redis 依赖
	"go.uber.org/zap"              // 添加 zap 导入
)

// ApplyNeo4jSchemaIfNeeded 是一个包装器，用于方便地从 bootstrap 调用
// 它创建一个临时 session 并调用 applyNeo4jSchema
// 注意：为了简化 bootstrap 中的逻辑，这里保留，但也可以直接在 bootstrap 中创建 session
func ApplyNeo4jSchemaIfNeeded(ctx context.Context, driver neo4j.DriverWithContext, logger *zap.Logger) error {
	session := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)

	return applyNeo4jSchema(ctx, session, logger)
}

// InitNeo4j 初始化 Neo4j 驱动并应用 Schema
// 返回创建好的驱动实例，如果初始化失败则返回错误。
func InitNeo4j(config Neo4jConfig) (neo4j.DriverWithContext, error) {
	// 假设已有 logger (这里用标准 log 模拟，实际应由调用者传入)
	tempLogger, _ := zap.NewProduction()
	defer tempLogger.Sync()

	driver, err := neo4j.NewDriverWithContext(
		config.URI,
		neo4j.BasicAuth(config.Username, config.Password, ""),
	)
	if err != nil {
		return nil, fmt.Errorf("无法创建 Neo4j 驱动: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := driver.VerifyConnectivity(ctx); err != nil {
		driver.Close(ctx)
		return nil, fmt.Errorf("无法连接到 Neo4j: %w", err)
	}
	tempLogger.Info("成功连接到 Neo4j (from InitNeo4j)")

	// 不再在此处应用 Schema
	// if err := ApplyNeo4jSchemaIfNeeded(ctx, driver, tempLogger); err != nil {
	// 	log.Printf("警告: 应用 Neo4j Schema 失败: %v", err)
	// } else {
	// 	log.Println("成功应用 Neo4j Schema")
	// }

	return driver, nil
}

// InitRedis 初始化 Redis 客户端连接
func InitRedis(config RedisConfig) (*redis.Client, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     config.Addr,
		Password: config.Password, // no password set
		DB:       config.DB,       // use default DB
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	status := rdb.Ping(ctx)
	if err := status.Err(); err != nil {
		return nil, fmt.Errorf("无法连接到 Redis (%s): %w", config.Addr, err)
	}

	// fmt.Printf("成功连接到 Redis (%s)\n", config.Addr) // 日志移到 bootstrap
	return rdb, nil
}

// applyNeo4jSchema 创建必要的索引和约束
// 修改为接受 session 和 logger
func applyNeo4jSchema(ctx context.Context, session neo4j.SessionWithContext, logger *zap.Logger) error {
	// 定义要应用的 Schema 查询语句
	queries := []string{
		// 节点 ID 唯一性约束 (如果节点类型确定，可以为特定类型创建)
		// "CREATE CONSTRAINT node_id_unique IF NOT EXISTS FOR (n) REQUIRE n.id IS UNIQUE", // 适用于所有节点
		"CREATE CONSTRAINT person_id_unique IF NOT EXISTS FOR (p:PERSON) REQUIRE p.id IS UNIQUE",
		"CREATE CONSTRAINT company_id_unique IF NOT EXISTS FOR (c:COMPANY) REQUIRE c.id IS UNIQUE",
		"CREATE CONSTRAINT school_id_unique IF NOT EXISTS FOR (s:SCHOOL) REQUIRE s.id IS UNIQUE",

		// 关系 ID 唯一性约束 (Neo4j 默认不支持直接对关系属性加唯一约束，通常关系ID业务生成并确保唯一)
		// 可以为关系属性创建索引以加速查找
		"CREATE INDEX relation_id_index IF NOT EXISTS FOR ()-[r]-() ON (r.id)",

		// 为常用查询字段创建索引
		"CREATE INDEX person_name_index IF NOT EXISTS FOR (p:PERSON) ON (p.name)",
		"CREATE INDEX person_profession_index IF NOT EXISTS FOR (p:PERSON) ON (p.profession)",
		"CREATE INDEX company_name_index IF NOT EXISTS FOR (c:COMPANY) ON (c.name)",
		"CREATE INDEX school_name_index IF NOT EXISTS FOR (s:SCHOOL) ON (s.name)",
	}

	logger.Info("开始应用 Neo4j schema...")

	// 在事务中执行每个 Schema 查询
	var appliedCount int
	for _, query := range queries {
		_, err := session.Run(ctx, query, nil)
		if err != nil {
			// 如果错误是约束或索引已存在，则忽略
			if strings.Contains(err.Error(), "already exists") || strings.Contains(err.Error(), "Constraint already exists") {
				logger.Debug("Schema (索引/约束) 已存在，跳过", zap.String("query", query))
				continue
			}
			logger.Error("执行 schema 查询失败", zap.String("query", query), zap.Error(err))
			return fmt.Errorf("执行 schema 查询失败 '%s': %w", query, err)
		}
		logger.Info("成功应用 schema", zap.String("query", query))
		appliedCount++
	}

	logger.Info("Neo4j schema 应用完成", zap.Int("applied_count", appliedCount))
	return nil
}
