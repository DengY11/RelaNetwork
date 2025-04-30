package database

// Neo4jConfig 定义了 Neo4j 连接所需的配置项
// 建议从配置文件或环境变量加载这些值
type Neo4jConfig struct {
	URI      string `mapstructure:"uri"` // 例如: bolt://localhost:7687
	Username string `mapstructure:"username"`
	Password string `mapstructure:"password"`
}

// 这里可以添加更多连接管理的逻辑，例如：
// - 获取单例驱动实例
// - 管理连接池状态等
