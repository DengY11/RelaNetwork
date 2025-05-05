package bootstrap

import (
	"context"
	"fmt"
	"log"

	"labelwall/biz/dal/neo4jdal"
	"labelwall/biz/handler/relationship/network" // 导入 handler 包
	"labelwall/biz/repo/neo4jrepo"
	"labelwall/biz/service"
	"labelwall/pkg/cache"
	"labelwall/pkg/config" // 导入配置包

	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/redis/go-redis/v9"
)

// Init 函数执行所有应用程序的初始化步骤
func Init(configPath string) (*server.Hertz, error) {
	// 1. 加载配置
	cfg, err := config.InitConfig(configPath)
	if err != nil {
		return nil, fmt.Errorf("加载配置失败: %w", err)
	}
	// TODO: 初始化日志库 (例如根据 cfg.Logging.Level)
	log.Println("Info: 配置加载完成.")

	// 2. 初始化数据库连接
	driver, err := InitDatabase(&cfg.Database.Neo4j)
	if err != nil {
		return nil, fmt.Errorf("初始化 Neo4j 失败: %w", err)
	}
	redisClient, err := InitRedis(&cfg.Database.Redis)
	if err != nil {
		return nil, fmt.Errorf("初始化 Redis 失败: %w", err)
	}
	log.Println("Info: 数据库连接初始化完成.")

	// 3. 初始化缓存
	appCache, err := InitCache(redisClient, &cfg.Cache)
	if err != nil {
		return nil, fmt.Errorf("初始化缓存失败: %w", err)
	}
	log.Println("Info: 缓存初始化完成.")

	// 4. 初始化 DAL
	nodeDAL, relationDAL := InitDALs()
	log.Println("Info: DAL 初始化完成.")

	// 5. 初始化 Repositories
	nodeRepo, relationRepo := InitRepositories(driver, appCache, nodeDAL, relationDAL)
	log.Println("Info: Repositories 初始化完成.")

	// 6. 初始化 Service
	networkSvc := InitService(nodeRepo, relationRepo)
	log.Println("Info: Service 初始化完成.")

	// 7. 注入依赖到 Handler
	InjectDependencies(networkSvc)
	log.Println("Info: 依赖注入 Handler 完成.")

	// 8. 初始化 Hertz 服务器 (不包括路由注册)
	h := server.New(
		server.WithHostPorts(cfg.Server.Address),
		// 添加其他 Hertz 服务器配置 (例如 From կոնֆիգ)
	)
	log.Println("Info: Hertz 服务器实例创建完成.")

	return h, nil // 返回 Hertz 实例，让 main 函数注册路由并启动
}

// InitDatabase 初始化 Neo4j 数据库连接
func InitDatabase(cfg *config.Neo4jConfig) (neo4j.DriverWithContext, error) {
	// 注意：这里不再需要将 config.Neo4jConfig 转换为 database.Neo4jConfig
	// 因为 database 包现在可以直接使用 config 包中的类型
	// 或者调整 database.InitNeo4j 的参数类型
	// 为了简单起见，我们直接在这里调用驱动创建逻辑，或者假设 database.InitNeo4j 接受 config.Neo4jConfig

	driver, err := neo4j.NewDriverWithContext(
		cfg.URI,
		neo4j.BasicAuth(cfg.Username, cfg.Password, ""),
	)
	if err != nil {
		return nil, fmt.Errorf("创建 Neo4j 驱动失败: %w", err)
	}
	if err := driver.VerifyConnectivity(context.Background()); err != nil {
		driver.Close(context.Background())
		return nil, fmt.Errorf("Neo4j 连接验证失败: %w", err)
	}
	// 可以在这里或 database 包中应用 Schema
	// err = database.ApplyNeo4jSchemaIfNeeded(driver) ...
	return driver, nil
}

// InitRedis 初始化 Redis 连接
func InitRedis(cfg *config.RedisConfig) (*redis.Client, error) {
	redisClient := redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,
	})
	if _, err := redisClient.Ping(context.Background()).Result(); err != nil {
		return nil, fmt.Errorf("Redis 连接 Ping 失败: %w", err)
	}
	return redisClient, nil
}

// InitCache 初始化应用缓存
func InitCache(redisClient *redis.Client, cfg *config.CacheConfig) (cache.NodeAndByteCache, error) {
	redisCache, err := cache.NewRedisCache(redisClient, cfg.Prefix, cfg.EstimatedKeys, cfg.FpRate)
	if err != nil {
		return nil, fmt.Errorf("创建 Redis 缓存实例失败: %w", err)
	}
	return redisCache, nil
}

// InitDALs 初始化数据访问层
func InitDALs() (neo4jdal.NodeDAL, neo4jdal.RelationDAL) {
	nodeDAL := neo4jdal.NewNodeDAL()
	relationDAL := neo4jdal.NewRelationDAL()
	return nodeDAL, relationDAL
}

func InitRepositories(driver neo4j.DriverWithContext, appCache cache.NodeAndByteCache, nodeDAL neo4jdal.NodeDAL, relationDAL neo4jdal.RelationDAL) (neo4jrepo.NodeRepository, neo4jrepo.RelationRepository) {
	// 类型断言：确保 appCache (其底层类型是 *RedisCache) 满足所需的接口
	relationCache, okRel := appCache.(cache.RelationAndByteCache)
	if !okRel {
		// 如果断言失败，意味着 NewRedisCache 的实现不满足接口，这是严重错误
		log.Fatal("严重: 初始化缓存未正确实现 RelationAndByteCache 接口")
	}
	nodeCache, okNode := appCache.(cache.NodeAndByteCache) // 这个断言总是成功的，因为 appCache 类型就是它
	if !okNode {
		log.Fatal("严重: 初始化缓存未正确实现 NodeAndByteCache 接口 (逻辑错误)")
	}

	relationRepo := neo4jrepo.NewRelationRepository(driver, relationDAL, relationCache)
	nodeRepo := neo4jrepo.NewNodeRepository(driver, nodeDAL, nodeCache, relationRepo)
	return nodeRepo, relationRepo
}

func InitService(nodeRepo neo4jrepo.NodeRepository, relationRepo neo4jrepo.RelationRepository) service.NetworkService {
	networkSvc := service.NewNetworkService(nodeRepo, relationRepo)
	return networkSvc
}

func InjectDependencies(networkSvc service.NetworkService) {
	network.SetNetworkService(networkSvc)
}
