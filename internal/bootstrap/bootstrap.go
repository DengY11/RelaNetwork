package bootstrap

import (
	"context"
	"fmt"
	"log"
	"time"

	"labelwall/biz/dal/neo4jdal"
	"labelwall/biz/handler/relationship/network" // 导入 handler 包
	"labelwall/biz/repo/neo4jrepo"
	"labelwall/biz/service"
	dbInfra "labelwall/infrastructure/database" // Alias database package
	"labelwall/pkg/cache"
	"labelwall/pkg/config" // 导入配置包

	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"         // 添加 zap 导入
	"go.uber.org/zap/zapcore" // <-- Import zapcore for level constants
)

// Init 函数执行所有应用程序的初始化步骤
func Init(configPath string) (*server.Hertz, error) {

	// 1. 加载配置 (移到最前)
	cfg, err := config.InitConfig(configPath)
	if err != nil {
		// 在 logger 初始化前，只能用标准 log
		log.Printf("Error: 加载配置失败: %v", err)
		return nil, fmt.Errorf("加载配置失败: %w", err)
	}
	log.Println("Info: 配置加载完成.") // 标准 log

	// 2. 初始化 Zap Logger (使用配置中的级别)
	var logger *zap.Logger
	var zapErr error
	logLevel := zapcore.InfoLevel // 默认为 Info
	switch cfg.Logging.Level {
	case "debug":
		logLevel = zapcore.DebugLevel
	case "info":
		logLevel = zapcore.InfoLevel
	case "warn":
		logLevel = zapcore.WarnLevel
	case "error":
		logLevel = zapcore.ErrorLevel
	default:
		log.Printf("Warning: 无效的日志级别 '%s' 在配置中，将使用 'info'", cfg.Logging.Level) // 标准 log
	}

	// 可以根据需要选择 Production 或 Development 配置
	// Development 模式更适合开发，输出更易读，包括调用者信息
	encoderConfig := zap.NewDevelopmentEncoderConfig() // Or zap.NewProductionEncoderConfig()
	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderConfig),   // Or NewConsoleEncoder
		zapcore.AddSync(log.Default().Writer()), // Write to standard log output
		logLevel,
	)
	logger = zap.New(core, zap.AddCaller()) // 添加 AddCaller 来显示文件名和行号

	if zapErr != nil { // Check potential errors during zap.New (though unlikely here)
		log.Fatalf("无法初始化 zap logger: %v", zapErr)
	}
	defer logger.Sync() // 确保缓冲区日志被写入

	logger.Info("Zap Logger 初始化完成", zap.String("level", cfg.Logging.Level)) // 现在可以使用 logger 了

	// 3. 初始化数据库连接 (传入配置好的 logger)
	driver, err := InitDatabase(logger, &cfg.Database.Neo4j)
	if err != nil {
		logger.Error("初始化 Neo4j 失败", zap.Error(err))
		return nil, fmt.Errorf("初始化 Neo4j 失败: %w", err)
	}
	redisClient, err := InitRedis(logger, &cfg.Database.Redis)
	if err != nil {
		logger.Error("初始化 Redis 失败", zap.Error(err))
		return nil, fmt.Errorf("初始化 Redis 失败: %w", err)
	}
	logger.Info("数据库连接初始化完成.")

	// 4. 初始化缓存
	appCache, err := InitCache(logger, redisClient, &cfg.Cache)
	if err != nil {
		logger.Error("初始化缓存失败", zap.Error(err))
		return nil, fmt.Errorf("初始化缓存失败: %w", err)
	}
	logger.Info("缓存初始化完成.")

	// 5. 初始化 DAL
	nodeDAL, relationDAL := InitDALs(logger)
	logger.Info("DAL 初始化完成.")

	// 6. 初始化 Repositories
	nodeRepo, relationRepo := InitRepositories(logger, driver, appCache, nodeDAL, relationDAL, &cfg.Cache, &cfg.Repo)
	logger.Info("Repositories 初始化完成.")

	// 7. 初始化 Service
	networkSvc := InitService(logger, nodeRepo, relationRepo)
	logger.Info("Service 初始化完成.")

	// 8. 注入依赖到 Handler
	InjectDependencies(logger, networkSvc)
	logger.Info("依赖注入 Handler 完成.")

	// 9. 初始化 Hertz 服务器 (不包括路由注册)
	h := server.New(
		server.WithHostPorts(cfg.Server.Address),
		// 添加其他 Hertz 服务器配置 (例如 From կոնֆիգ)
	)
	logger.Info("Hertz 服务器实例创建完成.")

	return h, nil // 返回 Hertz 实例，让 main 函数注册路由并启动
}

// InitDatabase 初始化 Neo4j 数据库连接
func InitDatabase(logger *zap.Logger, cfg *config.Neo4jConfig) (neo4j.DriverWithContext, error) {
	// 注意：不再需要调用旧的 database.InitNeo4j
	driver, err := neo4j.NewDriverWithContext(
		cfg.URI,
		neo4j.BasicAuth(cfg.Username, cfg.Password, ""),
	)
	if err != nil {
		return nil, fmt.Errorf("创建 Neo4j 驱动失败: %w", err)
	}

	// 使用 infrastructure/database 中的 ApplyNeo4jSchemaIfNeeded
	if err := dbInfra.ApplyNeo4jSchemaIfNeeded(context.Background(), driver, logger); err != nil {
		logger.Warn("应用 Neo4j Schema 期间发生错误 (详见 infrastructure/database 日志)", zap.Error(err))

	} else {
		logger.Info("Neo4j Schema 应用检查完成")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := driver.VerifyConnectivity(ctx); err != nil {
		driver.Close(ctx)
		return nil, fmt.Errorf("Neo4j 连接验证失败: %w", err)
	}
	logger.Info("成功验证 Neo4j 连接")

	return driver, nil
}

// InitRedis 初始化 Redis 连接
func InitRedis(logger *zap.Logger, cfg *config.RedisConfig) (*redis.Client, error) {
	redisClient := redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := redisClient.Ping(ctx).Result(); err != nil {
		return nil, fmt.Errorf("Redis 连接 Ping 失败: %w", err)
	}
	logger.Info("成功连接到 Redis", zap.String("address", cfg.Addr))
	return redisClient, nil
}

// InitCache 初始化应用缓存
func InitCache(logger *zap.Logger, redisClient *redis.Client, cfg *config.CacheConfig) (cache.NodeAndByteCache, error) {
	redisCache, err := cache.NewRedisCache(redisClient, cfg.Prefix, cfg.EstimatedKeys, cfg.FpRate)
	if err != nil {
		return nil, fmt.Errorf("创建 Redis 缓存实例失败: %w", err)
	}
	logger.Info("Redis 缓存实例创建成功")
	return redisCache, nil
}

// InitDALs 初始化数据访问层
func InitDALs(logger *zap.Logger) (neo4jdal.NodeDAL, neo4jdal.RelationDAL) {
	nodeDAL := neo4jdal.NewNodeDAL()
	relationDAL := neo4jdal.NewRelationDAL()
	logger.Info("NodeDAL 和 RelationDAL 创建成功")
	return nodeDAL, relationDAL
}

// InitRepositories 初始化仓库层
func InitRepositories(
	logger *zap.Logger, // 添加 logger 参数
	driver neo4j.DriverWithContext,
	appCache cache.NodeAndByteCache,
	nodeDAL neo4jdal.NodeDAL,
	relationDAL neo4jdal.RelationDAL,
	cacheCfg *config.CacheConfig,
	repoCfg *config.RepoConfig,
) (neo4jrepo.NodeRepository, neo4jrepo.RelationRepository) {
	relationCache, okRel := appCache.(cache.RelationAndByteCache)
	if !okRel {
		logger.Fatal("初始化缓存未正确实现 RelationAndByteCache 接口") // 使用 logger.Fatal
	}
	nodeCache, okNode := appCache.(cache.NodeAndByteCache)
	if !okNode {
		logger.Fatal("初始化缓存未正确实现 NodeAndByteCache 接口 (逻辑错误)") // 使用 logger.Fatal
	}

	relationRepo := neo4jrepo.NewRelationRepository(
		driver,
		relationDAL,
		relationCache,
		cacheCfg.TTL.DefaultRelation,
		cacheCfg.TTL.GetNodeRelations,
		logger,
	)
	logger.Info("RelationRepository 创建成功")

	nodeRepo := neo4jrepo.NewNodeRepository(
		driver,
		nodeDAL,
		nodeCache,
		relationRepo,
		cacheCfg.TTL.DefaultNode,
		cacheCfg.TTL.SearchNodes,
		cacheCfg.TTL.GetNetwork,
		cacheCfg.TTL.GetPath,
		repoCfg.QueryParams.GetNetworkMaxDepth,
		repoCfg.QueryParams.GetPathMaxDepth,
		repoCfg.QueryParams.GetPathMaxDepthLimit,
		repoCfg.QueryParams.SearchNodesDefaultLimit,
		logger,
	)
	logger.Info("NodeRepository 创建成功")

	return nodeRepo, relationRepo
}

// InitService 初始化服务层
func InitService(logger *zap.Logger, nodeRepo neo4jrepo.NodeRepository, relationRepo neo4jrepo.RelationRepository) service.NetworkService {
	networkSvc := service.NewNetworkService(nodeRepo, relationRepo, logger)
	logger.Info("NetworkService 创建成功")
	return networkSvc
}

// InjectDependencies 注入依赖到 Handler
// 同时注入 logger
func InjectDependencies(logger *zap.Logger, networkSvc service.NetworkService) {
	network.SetNetworkService(networkSvc, logger) // 将 logger 传递给 SetNetworkService
	logger.Info("NetworkService 和 Logger 成功注入到 Network Handler")
}
