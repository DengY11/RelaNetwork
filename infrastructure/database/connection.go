package database

// Neo4jConfig 定义了 Neo4j 连接所需的配置项
// 建议从配置文件或环境变量加载这些值
type Neo4jConfig struct {
	URI      string `mapstructure:"uri"` // 例如: bolt://localhost:7687
	Username string `mapstructure:"username"`
	Password string `mapstructure:"password"`
}

// RedisConfig 保存 Redis 连接所需的配置信息
type RedisConfig struct {
	Addr     string // 例如 "localhost:6379"
	Password string // 如果没有密码则为空
	DB       int    // 例如 0
}

//
// TODO 获取单例驱动实例
// TODO 管理连接池状态等
