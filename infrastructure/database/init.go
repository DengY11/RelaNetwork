package database

//TODO: 使用更专业的日志库
//TODO: 从配置文件中读取redis和neo4j的配置

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/redis/go-redis/v9" // 添加 redis 依赖
	"go.uber.org/zap"              // 添加 zap 导入
)

// ApplyNeo4jSchemaIfNeeded 是一个辅助函数，允许从外部传入 logger 调用 applyNeo4jSchema
func ApplyNeo4jSchemaIfNeeded(ctx context.Context, driver neo4j.DriverWithContext, logger *zap.Logger) error {
	session := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)
	return applyNeo4jSchema(ctx, session, logger) // 调用修改后的 applyNeo4jSchema
}

// InitNeo4j 初始化 Neo4j 驱动并应用 Schema
// 返回创建好的驱动实例，如果初始化失败则返回错误。
func InitNeo4j(config Neo4jConfig) (neo4j.DriverWithContext, error) {
	// 使用配置创建驱动
	driver, err := neo4j.NewDriverWithContext(
		config.URI,
		neo4j.BasicAuth(config.Username, config.Password, ""),
	)
	if err != nil {
		return nil, fmt.Errorf("无法创建 Neo4j 驱动: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 检查连接性
	if err := driver.VerifyConnectivity(ctx); err != nil {
		driver.Close(ctx) // 关闭无效的驱动
		return nil, fmt.Errorf("无法连接到 Neo4j: %w", err)
	}
	log.Println("成功连接到 Neo4j") // 保留 log.Println

	// 应用 Schema (索引和约束) - 使用内部未修改的 applyNeo4jSchema
	// 注意：这里调用的是只需要 driver 的旧版签名，但内部实现将被下面的 zap 版本覆盖
	// 为了最小化改动，暂时保留此调用结构，但 bootstrap 初始化时应使用 ApplyNeo4jSchemaIfNeeded
	if err := applyNeo4jSchemaLegacy(ctx, driver); err != nil {
		// 通常不应因为 Schema 初始化失败而停止服务，记录警告即可
		log.Printf("警告: 应用 Neo4j Schema 失败: %v", err) // 保留 log.Printf
	} else {
		log.Println("成功应用 Neo4j Schema") // 保留 log.Println
	}

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

	fmt.Printf("成功连接到 Redis (%s)\n", config.Addr) // 保留 fmt.Printf
	return rdb, nil
}

// applyNeo4jSchemaLegacy 是原始的 applyNeo4jSchema 实现，用于 InitNeo4j 内部调用
// 它不接受 logger 参数，使用标准的 log/fmt
func applyNeo4jSchemaLegacy(ctx context.Context, driver neo4j.DriverWithContext) error {
	session := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)

	queries := []string{
		"CREATE CONSTRAINT person_id_unique IF NOT EXISTS FOR (p:PERSON) REQUIRE p.id IS UNIQUE",
		"CREATE CONSTRAINT company_id_unique IF NOT EXISTS FOR (c:COMPANY) REQUIRE c.id IS UNIQUE",
		"CREATE CONSTRAINT school_id_unique IF NOT EXISTS FOR (s:SCHOOL) REQUIRE s.id IS UNIQUE",
		"CREATE INDEX relation_id_index IF NOT EXISTS FOR ()-[r]-() ON (r.id)",
		"CREATE INDEX person_name_index IF NOT EXISTS FOR (p:PERSON) ON (p.name)",
		"CREATE INDEX person_profession_index IF NOT EXISTS FOR (p:PERSON) ON (p.profession)",
		"CREATE INDEX company_name_index IF NOT EXISTS FOR (c:COMPANY) ON (c.name)",
		"CREATE INDEX school_name_index IF NOT EXISTS FOR (s:SCHOOL) ON (s.name)",
	}

	for _, query := range queries {
		_, err := session.Run(ctx, query, nil)
		if err != nil {
			if strings.Contains(err.Error(), "already exists") || strings.Contains(err.Error(), "Constraint already exists") {
				fmt.Printf("Schema (索引/约束) 已存在，跳过: %s\n", query) // 保留 fmt.Printf
				continue
			}
			return fmt.Errorf("执行 schema 查询失败 '%s': %w", query, err)
		}
		fmt.Printf("成功应用 schema: %s\n", query) // 保留 fmt.Printf
	}

	fmt.Println("Neo4j schema 应用完成") // 保留 fmt.Println
	return nil
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

	logger.Info("开始应用 Neo4j schema...") // 使用 zap logger

	// 在事务中执行每个 Schema 查询
	var appliedCount int
	for _, query := range queries {
		_, err := session.Run(ctx, query, nil)
		if err != nil {
			// 如果错误是约束或索引已存在，则忽略
			if strings.Contains(err.Error(), "already exists") || strings.Contains(err.Error(), "Constraint already exists") {
				// 使用 zap logger 替换 fmt.Printf
				logger.Debug("Schema (索引/约束) 已存在，跳过", zap.String("query", query))
				continue
			}
			// 使用 zap logger 记录错误
			logger.Error("执行 schema 查询失败", zap.String("query", query), zap.Error(err))
			return fmt.Errorf("执行 schema 查询失败 '%s': %w", query, err)
		}
		// 使用 zap logger 替换 fmt.Printf
		logger.Info("成功应用 schema", zap.String("query", query))
		appliedCount++
	}

	// 使用 zap logger 替换 fmt.Println
	logger.Info("Neo4j schema 应用完成", zap.Int("applied_count", appliedCount))
	return nil
}
