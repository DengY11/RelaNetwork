package config

import (
	"fmt"
	"log"

	"github.com/fsnotify/fsnotify"
	"github.com/spf13/viper"
)

// AppConfig 包含所有应用程序的配置
type AppConfig struct {
	Server   ServerConfig   `mapstructure:"server"`
	Database DatabaseConfig `mapstructure:"database"`
	Cache    CacheConfig    `mapstructure:"cache"`
	Logging  LoggingConfig  `mapstructure:"logging"`
	Repo     RepoConfig     `mapstructure:"repository"`
	RabbitMQ RabbitMQConfig `mapstructure:"rabbitmq"`
}

// ServerConfig 服务器相关配置
type ServerConfig struct {
	Address string `mapstructure:"address"`
}

// DatabaseConfig 包含所有数据库的配置
type DatabaseConfig struct {
	Neo4j Neo4jConfig `mapstructure:"neo4j"`
	Redis RedisConfig `mapstructure:"redis"`
}

// Neo4jConfig Neo4j 连接配置
type Neo4jConfig struct {
	URI                          string `mapstructure:"uri"`
	Username                     string `mapstructure:"username"`
	Password                     string `mapstructure:"password"`
	MaxConnectionPoolSize        int    `mapstructure:"max_connection_pool_size"`               // 最大连接池大小
	ConnectionAcquisitionTimeout int    `mapstructure:"connection_acquisition_timeout_seconds"` // 连接获取超时时间（秒）
	MaxConnectionLifetime        int    `mapstructure:"max_connection_lifetime_seconds"`        // 连接最大生命周期（秒）
}

// RedisConfig Redis 连接配置
type RedisConfig struct {
	Addr     string `mapstructure:"addr"`
	Password string `mapstructure:"password"`
	DB       int    `mapstructure:"db"`
}

// CacheConfig 缓存相关配置
type CacheConfig struct {
	Prefix        string         `mapstructure:"prefix"`
	EstimatedKeys uint           `mapstructure:"estimated_keys"`
	FpRate        float64        `mapstructure:"fp_rate"`
	TTL           CacheTTLConfig `mapstructure:"ttl"`
}

// CacheTTLConfig 缓存过期时间配置 (单位：秒)
type CacheTTLConfig struct {
	DefaultNode      int `mapstructure:"default_node"`
	DefaultRelation  int `mapstructure:"default_relation"`
	SearchNodes      int `mapstructure:"search_nodes"`
	GetNetwork       int `mapstructure:"get_network"`
	GetPath          int `mapstructure:"get_path"`
	GetNodeRelations int `mapstructure:"get_node_relations"`
	// EmptyPlaceholder int `mapstructure:"empty_placeholder"` // 如需配置空值 TTL
}

// RepoConfig 仓库层相关配置
type RepoConfig struct {
	QueryParams RepoQueryConfig `mapstructure:"query_params"`
}

// RepoQueryConfig 仓库查询参数配置
type RepoQueryConfig struct {
	GetNetworkMaxDepth           int `mapstructure:"get_network_max_depth"`
	GetPathMaxDepth              int `mapstructure:"get_path_max_depth"`
	GetPathMaxDepthLimit         int `mapstructure:"get_path_max_depth_limit"`
	SearchNodesDefaultLimit      int `mapstructure:"search_nodes_default_limit"`
	GetNodeRelationsDefaultLimit int `mapstructure:"get_node_relations_default_limit"`
}

// LoggingConfig 日志相关配置
type LoggingConfig struct {
	Level string `mapstructure:"level"`
}

// RabbitMQConfig RabbitMQ 连接配置 (新增)
type RabbitMQConfig struct {
	Enabled bool   `mapstructure:"enabled"`
	URL     string `mapstructure:"url"`
	VHost   string `mapstructure:"vhost"`
	// ConnectionPoolSize int `mapstructure:"connection_pool_size"` // 可选，未来可添加
}

// GlobalConfig 是全局配置实例
var GlobalConfig = new(AppConfig)

func InitConfig(path string) (*AppConfig, error) {
	v := viper.New()
	v.SetConfigFile(path)   // 设置配置文件路径
	v.SetConfigType("yaml") // 设置配置文件类型

	// 尝试读取配置文件
	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("读取配置文件失败: %w", err)
	}

	// 将配置解析到 GlobalConfig 结构体中
	if err := v.Unmarshal(GlobalConfig); err != nil {
		return nil, fmt.Errorf("解析配置文件失败: %w", err)
	}

	// 监听配置文件变化 (可选)
	v.WatchConfig()
	v.OnConfigChange(func(e fsnotify.Event) {
		log.Printf("配置文件已更改: %s", e.Name)
		// 重新解析配置到 GlobalConfig
		if err := v.Unmarshal(GlobalConfig); err != nil {
			log.Printf("警告: 重新解析配置文件失败: %v", err)
		} else {
			log.Println("Info: 配置已重新加载.")
		}
	})

	log.Printf("Info: 成功加载并解析配置文件: %s", path)
	return GlobalConfig, nil
}
