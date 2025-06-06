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
	"labelwall/infrastructure/rabbitmq"         // <--- 新增 RabbitMQ 包导入
	"labelwall/pkg/cache"
	"labelwall/pkg/config" // 导入配置包

	"github.com/cloudwego/hertz/pkg/app/server"
	prometheus "github.com/hertz-contrib/monitor-prometheus" // 新增 Prometheus 监控包导入
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"         // 添加 zap 导入
	"go.uber.org/zap/zapcore" // <-- Import zapcore for level constants
)

// Init 函数执行所有应用程序的初始化步骤
// 返回 Hertz 实例、RabbitMQ Publisher (如果启用) 和错误
func Init(configPath string) (*server.Hertz, *rabbitmq.Publisher, error) {

	cfg, err := config.InitConfig(configPath) //加载配置
	if err != nil {
		log.Printf("Error: 加载配置失败: %v", err)
		return nil, nil, fmt.Errorf("加载配置失败: %w", err)
	}
	log.Println("Info: 配置加载完成.")

	var logger *zap.Logger
	logLevel := zapcore.InfoLevel
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

	defer func(logger *zap.Logger) {
		err := logger.Sync()
		if err != nil {
			log.Printf("error: zap logger缓冲区日志写入失败！")
		}
	}(logger) // 确保缓冲区日志被写入

	logger.Info("Zap Logger 初始化完成", zap.String("level", cfg.Logging.Level))

	// 初始化 RabbitMQ Publisher (如果已在配置中启用)
	var publisher *rabbitmq.Publisher
	if cfg.RabbitMQ.Enabled {
		logger.Info("RabbitMQ 已启用，正在初始化 Publisher...", zap.String("url", cfg.RabbitMQ.URL))
		// 确保 URL 格式正确，如果 vhost 在 URL 中没有，则附加它
		// RabbitMQ URL 格式: amqp://user:pass@host:port/vhost
		// 当前 config.yaml 中的 URL "amqp://labelwall_user:labelwall_pass@rabbitmq:5672/" 假定 vhost 是 "/"
		// 如果 cfg.RabbitMQ.VHost 非空且不是 "/"，您可能需要构建 URL
		// 例如: amqpURL := fmt.Sprintf("%s%s", cfg.RabbitMQ.URL, cfg.RabbitMQ.VHost)
		// 这里我们假设 cfg.RabbitMQ.URL 已经包含了 VHost (如果需要) 或者 VHost 是默认的 "/"

		// 暂时使用一个硬编码的 exchange name，后续可以考虑配置化
		exchangeName := "labelwall_exchange"

		publisher, err = rabbitmq.NewPublisher(cfg.RabbitMQ.URL, exchangeName, logger)
		if err != nil {
			logger.Error("初始化 RabbitMQ Publisher 失败", zap.Error(err))
			// 根据策略，这里可以选择返回错误，或者仅记录并继续 (如果 MQ 不是严格必需的)
			// 为了安全起见，我们返回错误
			return nil, nil, fmt.Errorf("初始化 RabbitMQ Publisher 失败: %w", err)
		}
		logger.Info("RabbitMQ Publisher 初始化成功", zap.String("exchange", exchangeName))
	} else {
		logger.Info("RabbitMQ 未在配置中启用。")
	}

	// 3.连接数据库
	driver, err := InitDatabase(logger, &cfg.Database.Neo4j)
	if err != nil {
		logger.Error("初始化 Neo4j 失败", zap.Error(err))
		if publisher != nil { // 如果 publisher 已初始化，尝试关闭
			publisher.Close()
		}
		return nil, nil, fmt.Errorf("初始化 Neo4j 失败: %w", err)
	}
	redisClient, err := InitRedis(logger, &cfg.Database.Redis)
	if err != nil {
		logger.Error("初始化 Redis 失败", zap.Error(err))
		if publisher != nil {
			publisher.Close()
		}
		return nil, nil, fmt.Errorf("初始化 Redis 失败: %w", err)
	}
	logger.Info("数据库连接初始化完成.")

	// 4. 初始化缓存
	appCache, err := InitCache(logger, redisClient, &cfg.Cache)
	if err != nil {
		logger.Error("初始化缓存失败", zap.Error(err))
		if publisher != nil {
			publisher.Close()
		}
		return nil, nil, fmt.Errorf("初始化缓存失败: %w", err)
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
		// 添加 Prometheus Tracer
		server.WithTracer(prometheus.NewServerTracer(":9091", "/metrics")),
		// 添加其他 Hertz 服务器配置 (例如 From կոնֆիգ)
	)
	logger.Info("Hertz 服务器实例创建完成.")
	logger.Info("Prometheus metrics 将在 :9091/metrics 路径暴露.")

	return h, publisher, nil // 返回 Hertz 实例、publisher 和 nil 错误
}

// InitDatabase 初始化 Neo4j 数据库连接
func InitDatabase(logger *zap.Logger, cfg *config.Neo4jConfig) (neo4j.DriverWithContext, error) {
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
		err := driver.Close(ctx)
		if err != nil {
			return nil, err
		}
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

// InjectDependencies 注入依赖(service和logger)到 Handler
func InjectDependencies(logger *zap.Logger, networkSvc service.NetworkService) {
	network.SetNetworkService(networkSvc, logger) // 将 logger 传递给 SetNetworkService
	logger.Info("NetworkService 和 Logger 成功注入到 Network Handler")
}
