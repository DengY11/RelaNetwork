package database

//TODO: 使用更专业的日志库
//TODO: 从配置文件中读取redis和neo4j的配置

import (
	"context"
	"fmt"
	"labelwall/pkg/config" // 确保导入我们修改的配置包
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
// 修改为使用 pkg/config.Neo4jConfig 并应用连接池配置
func InitNeo4j(cfg *config.Neo4jConfig) (neo4j.DriverWithContext, error) {
	if cfg == nil {
		return nil, fmt.Errorf("Neo4j 配置不能为空")
	}
	auth := neo4j.BasicAuth(cfg.Username, cfg.Password, "")

	// 创建驱动，并传入配置函数
	driver, err := neo4j.NewDriverWithContext(cfg.URI, auth, func(neo4jCfg *neo4j.Config) {
		// 应用连接池配置
		if cfg.MaxConnectionPoolSize > 0 {
			neo4jCfg.MaxConnectionPoolSize = cfg.MaxConnectionPoolSize
			log.Printf("Info: Neo4j MaxConnectionPoolSize 设置为: %d", cfg.MaxConnectionPoolSize)
		} else {
			log.Printf("Info: Neo4j MaxConnectionPoolSize 使用驱动默认值 (当前配置值: %d)", cfg.MaxConnectionPoolSize)
		}

		if cfg.ConnectionAcquisitionTimeout > 0 {
			neo4jCfg.ConnectionAcquisitionTimeout = time.Duration(cfg.ConnectionAcquisitionTimeout) * time.Second
			log.Printf("Info: Neo4j ConnectionAcquisitionTimeout 设置为: %v", neo4jCfg.ConnectionAcquisitionTimeout)
		} else {
			log.Printf("Info: Neo4j ConnectionAcquisitionTimeout 使用驱动默认值 (当前配置值: %d s)", cfg.ConnectionAcquisitionTimeout)
		}

		if cfg.MaxConnectionLifetime > 0 {
			neo4jCfg.MaxConnectionLifetime = time.Duration(cfg.MaxConnectionLifetime) * time.Second
			log.Printf("Info: Neo4j MaxConnectionLifetime 设置为: %v", neo4jCfg.MaxConnectionLifetime)
		} else {
			log.Printf("Info: Neo4j MaxConnectionLifetime 使用驱动默认值 (通常是无限，当前配置值: %d s)", cfg.MaxConnectionLifetime)
		}
		// neo4jCfg.UserAgent = "labelwall/1.0.0" // 可选: 设置 UserAgent
	})

	if err != nil {
		return nil, fmt.Errorf("无法创建 Neo4j 驱动: %w", err)
	}

	// 验证连接性
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := driver.VerifyConnectivity(ctx); err != nil {
		driver.Close(ctx)
		return nil, fmt.Errorf("无法连接到 Neo4j: %w", err)
	}
	log.Println("成功连接到 Neo4j")

	// 应用 Schema (假设总是需要)
	if err := applyNeo4jSchemaLegacy(ctx, driver); err != nil {
		log.Printf("警告: 应用 Neo4j Schema 失败: %v", err)
	} else {
		log.Println("成功应用 Neo4j Schema")
	}

	return driver, nil
}

// InitRedis 初始化 Redis 客户端连接
func InitRedis(cfg *config.RedisConfig) (*redis.Client, error) {
	if cfg == nil {
		return nil, fmt.Errorf("Redis 配置不能为空")
	}
	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password, // no password set
		DB:       cfg.DB,       // use default DB
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	status := rdb.Ping(ctx)
	if err := status.Err(); err != nil {
		return nil, fmt.Errorf("无法连接到 Redis (%s): %w", cfg.Addr, err)
	}

	fmt.Printf("成功连接到 Redis (%s)\n", cfg.Addr) // 保留 fmt.Printf
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
				fmt.Printf("Schema (索引/约束) 已存在，跳过: %s\n", query)
				continue
			}
			if strings.Contains(err.Error(), "Invalid input") && strings.Contains(query, "relation_id_index") {
				log.Printf("警告: 创建关系索引 '%s' 时可能存在语法问题。错误: %v. 请检查你的 Neo4j 版本对应的正确语法。", query, err)
				// continue // or return error
			}
			return fmt.Errorf("执行 schema 查询失败 '%s': %w", query, err)
		}
		fmt.Printf("成功应用 schema: %s\n", query)
	}

	fmt.Println("Neo4j schema 应用完成")
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
			if strings.Contains(err.Error(), "Invalid input") && strings.Contains(query, "relation_id_index") {
				logger.Warn("创建关系索引时可能存在语法问题", zap.String("query", query), zap.Error(err), zap.String("suggestion", "请检查你的 Neo4j 版本对应的正确语法。"))
				// continue
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
