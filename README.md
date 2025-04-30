# LabelWall 关系网络服务

## 1. 项目概述

### 1.1 项目简介

关系网络服务是一个专门设计用于管理、查询和可视化复杂社交和组织关系网络的后端服务。该服务提供了全面的 API 接口，支持节点（人、公司、学校等）和关系（同事、校友、朋友等）的创建、读取、更新和删除操作，以及基于图形数据库的高级网络查询和路径分析功能。

### 1.2 核心功能

- **节点管理**：创建、更新、查询和删除各种类型的节点（人、公司、学校等）
- **关系管理**：创建、更新、查询和删除不同类型的关系（同事、校友、朋友、访问等）
- **网络查询**：根据职业等属性查询相关节点及其关系网络
- **路径分析**：查找两个节点之间的关系路径，支持多级关系分析
- **高级搜索**：支持基于关键词、节点类型等条件的节点搜索
- **关系过滤**：支持按关系类型、方向等条件筛选节点关系

### 1.3 应用场景

- **人脉管理系统**：可视化用户的社交关系网络
- **企业组织结构**：展示公司内部或跨公司的人员关系
- **校友网络**：构建和分析学校校友之间的关系
- **行业关系图谱**：展示特定行业中的人员、企业间关联
- **数据可视化应用**：为前端提供结构化的关系数据，支持图形化展示

## 2. 技术栈

### 2.1 核心框架

- **Hertz**: 字节跳动开源的高性能 Go HTTP 框架，用于构建 API 服务
- **Thrift**: 跨语言的 RPC 框架，用于定义服务接口和数据结构
- **Go**: 后端开发语言，版本 1.16+

### 2.2 数据存储

- **图数据库** (如 Neo4j): 用于存储和查询关系网络数据
- **Redis**: 可选组件，用于缓存热门查询结果

### 2.3 开发工具

- **hz**: Hertz 代码生成工具，用于从 IDL 生成服务代码
- **thriftgo**: Thrift IDL 编译器，生成 Thrift 相关代码

## 3. 系统架构

### 3.1 整体架构

```
+----------------+     +-------------------+     +------------------+
|                |     |                   |     |                  |
|  客户端应用    +---->+  LabelWall API    +---->+  图数据库       |
|  (Web/Mobile)  |     |  (Hertz/Thrift)   |     |  (Neo4j)         |
|                |     |                   |     |                  |
+----------------+     +-------------------+     +------------------+
                                |
                                v
                        +---------------+
                        |               |
                        |  缓存层       |
                        |  (Redis)      |
                        |               |
                        +---------------+
```

### 3.2 模块划分

- **API 模块**: 处理 HTTP 请求/响应，参数验证，路由转发
- **服务层**: 实现业务逻辑，数据转换，调用数据访问层
- **数据访问层**: 与图数据库交互，执行查询和更新操作
- **模型层**: 定义数据结构和枚举类型

## 4. 数据模型

### 4.1 节点类型 (NodeType)

节点代表网络中的实体，可以是人、公司或学校等。

```
enum NodeType {
    PERSON = 1
    COMPANY = 2
    SCHOOL = 3
}
```

### 4.2 关系类型 (RelationType)

关系表示节点之间的连接，如同事、校友、朋友等。

```
enum RelationType {
    COLLEAGUE = 1     // 同事
    SCHOOLMATE = 2    // 校友
    FRIEND = 3        // 朋友
    VISITED = 4       // 访问过
    FOLLOWING = 5     // 关注
}
```

### 4.3 节点结构 (Node)

```
struct Node {
    1: string id              // 节点ID
    2: NodeType type          // 节点类型
    3: string name            // 名称
    4: optional string avatar // 头像URL
    5: optional string profession // 职业
    6: optional map<string, string> properties // 其他属性
}
```

### 4.4 关系结构 (Relation)

```
struct Relation {
    1: string id              // 关系ID
    2: string source          // 起始节点ID
    3: string target          // 目标节点ID
    4: RelationType type      // 关系类型
    5: optional string label  // 关系标签
    6: optional map<string, string> properties // 关系属性
}
```

## 5. API 详细说明

### 5.1 节点管理 API

#### 5.1.1 创建节点

- **端点**: `POST /api/v1/nodes`
- **描述**: 创建一个新节点
- **请求参数**:
  ```json
  {
    "type": 1,             // NodeType: 1=人物, 2=公司, 3=学校
    "name": "张三",        // 节点名称
    "avatar": "http://example.com/avatar.jpg", // 可选，头像URL
    "profession": "工程师", // 可选，职业
    "properties": {        // 可选，其他属性
      "age": "30",
      "location": "北京"
    }
  }
  ```
- **响应**:
  ```json
  {
    "success": true,
    "message": "节点创建成功",
    "node": {
      "id": "node123",
      "type": 1,
      "name": "张三",
      "avatar": "http://example.com/avatar.jpg",
      "profession": "工程师",
      "properties": {
        "age": "30",
        "location": "北京"
      }
    }
  }
  ```

#### 5.1.2 获取节点

- **端点**: `GET /api/v1/nodes/:id`
- **描述**: 获取特定节点的详细信息
- **路径参数**: `id` - 节点ID
- **响应**:
  ```json
  {
    "success": true,
    "message": "节点获取成功",
    "node": {
      "id": "node123",
      "type": 1,
      "name": "张三",
      "avatar": "http://example.com/avatar.jpg",
      "profession": "工程师",
      "properties": {
        "age": "30",
        "location": "北京"
      }
    }
  }
  ```

#### 5.1.3 更新节点

- **端点**: `PUT /api/v1/nodes/:id`
- **描述**: 更新特定节点的信息
- **路径参数**: `id` - 节点ID
- **请求参数**:
  ```json
  {
    "name": "张三丰",      // 可选，更新名称
    "avatar": "http://example.com/new-avatar.jpg", // 可选，更新头像
    "profession": "高级工程师", // 可选，更新职业
    "properties": {        // 可选，更新其他属性
      "age": "32",
      "location": "上海"
    }
  }
  ```
- **响应**:
  ```json
  {
    "success": true,
    "message": "节点更新成功",
    "node": {
      "id": "node123",
      "type": 1,
      "name": "张三丰",
      "avatar": "http://example.com/new-avatar.jpg",
      "profession": "高级工程师",
      "properties": {
        "age": "32",
        "location": "上海"
      }
    }
  }
  ```

#### 5.1.4 删除节点

- **端点**: `DELETE /api/v1/nodes/:id`
- **描述**: 删除特定节点
- **路径参数**: `id` - 节点ID
- **响应**:
  ```json
  {
    "success": true,
    "message": "节点删除成功"
  }
  ```

#### 5.1.5 搜索节点

- **端点**: `GET /api/v1/nodes/search`
- **描述**: 根据关键词搜索节点
- **查询参数**:
    - `keyword` - 搜索关键词
    - `type` - 可选，节点类型
    - `limit` - 可选，返回结果数量限制
    - `offset` - 可选，分页偏移量
- **响应**:
  ```json
  {
    "success": true,
    "message": "搜索成功",
    "nodes": [
      {
        "id": "node123",
        "type": 1,
        "name": "张三丰",
        "avatar": "http://example.com/avatar1.jpg",
        "profession": "工程师"
      },
      {
        "id": "node456",
        "type": 1,
        "name": "张三",
        "avatar": "http://example.com/avatar2.jpg",
        "profession": "设计师"
      }
    ],
    "total": 2
  }
  ```

### 5.2 关系管理 API

#### 5.2.1 创建关系

- **端点**: `POST /api/v1/relations`
- **描述**: 创建两个节点之间的关系
- **请求参数**:
  ```json
  {
    "source": "node123",   // 源节点ID
    "target": "node456",   // 目标节点ID
    "type": 1,             // RelationType: 1=同事, 2=校友, 3=朋友, 4=访问, 5=关注
    "label": "前同事",     // 可选，关系标签
    "properties": {        // 可选，关系属性
      "since": "2020",
      "company": "ABC公司"
    }
  }
  ```
- **响应**:
  ```json
  {
    "success": true,
    "message": "关系创建成功",
    "relation": {
      "id": "rel123",
      "source": "node123",
      "target": "node456",
      "type": 1,
      "label": "前同事",
      "properties": {
        "since": "2020",
        "company": "ABC公司"
      }
    }
  }
  ```

#### 5.2.2 获取关系

- **端点**: `GET /api/v1/relations/:id`
- **描述**: 获取特定关系的详细信息
- **路径参数**: `id` - 关系ID
- **响应**:
  ```json
  {
    "success": true,
    "message": "关系获取成功",
    "relation": {
      "id": "rel123",
      "source": "node123",
      "target": "node456",
      "type": 1,
      "label": "前同事",
      "properties": {
        "since": "2020",
        "company": "ABC公司"
      }
    }
  }
  ```

#### 5.2.3 更新关系

- **端点**: `PUT /api/v1/relations/:id`
- **描述**: 更新特定关系的信息
- **路径参数**: `id` - 关系ID
- **请求参数**:
  ```json
  {
    "type": 3,             // 可选，更新关系类型为朋友
    "label": "好友",       // 可选，更新关系标签
    "properties": {        // 可选，更新关系属性
      "since": "2018",
      "closer": "true"
    }
  }
  ```
- **响应**:
  ```json
  {
    "success": true,
    "message": "关系更新成功",
    "relation": {
      "id": "rel123",
      "source": "node123",
      "target": "node456",
      "type": 3,
      "label": "好友",
      "properties": {
        "since": "2018",
        "closer": "true"
      }
    }
  }
  ```

#### 5.2.4 删除关系

- **端点**: `DELETE /api/v1/relations/:id`
- **描述**: 删除特定关系
- **路径参数**: `id` - 关系ID
- **响应**:
  ```json
  {
    "success": true,
    "message": "关系删除成功"
  }
  ```

#### 5.2.5 获取节点关系

- **端点**: `GET /api/v1/nodes/:node_id/relations`
- **描述**: 获取特定节点的所有关系
- **路径参数**: `node_id` - 节点ID
- **查询参数**:
    - `types` - 可选，关系类型列表，以逗号分隔
    - `outgoing` - 可选，是否包含出去的关系，默认true
    - `incoming` - 可选，是否包含进来的关系，默认true
    - `limit` - 可选，返回结果数量限制
    - `offset` - 可选，分页偏移量
- **响应**:
  ```json
  {
    "success": true,
    "message": "关系获取成功",
    "relations": [
      {
        "id": "rel123",
        "source": "node123",
        "target": "node456",
        "type": 1,
        "label": "同事"
      },
      {
        "id": "rel789",
        "source": "node789",
        "target": "node123",
        "type": 3,
        "label": "朋友"
      }
    ],
    "total": 2
  }
  ```

### 5.3 网络查询 API

#### 5.3.1 职业网络查询

- **端点**: `GET /api/v1/network`
- **描述**: 根据职业查询相关节点及其关系网络
- **查询参数**:
    - `profession` - 职业名称
    - `depth` - 可选，查询深度，默认为1
- **响应**:
  ```json
  {
    "success": true,
    "message": "网络查询成功",
    "nodes": [
      {
        "id": "node123",
        "type": 1,
        "name": "张三",
        "profession": "工程师"
      },
      {
        "id": "node456",
        "type": 1,
        "name": "李四",
        "profession": "工程师"
      }
    ],
    "relations": [
      {
        "id": "rel123",
        "source": "node123",
        "target": "node456",
        "type": 1,
        "label": "同事"
      }
    ]
  }
  ```

#### 5.3.2 路径查询

- **端点**: `GET /api/v1/path`
- **描述**: 查询两个节点之间的关系路径
- **查询参数**:
    - `source_id` - 起始节点ID
    - `target_id` - 目标节点ID
    - `max_depth` - 可选，最大查询深度，默认为3
    - `types` - 可选，关系类型列表，以逗号分隔
- **响应**:
  ```json
  {
    "success": true,
    "message": "路径查询成功",
    "nodes": [
      {
        "id": "node123",
        "type": 1,
        "name": "张三",
        "profession": "工程师"
      },
      {
        "id": "node789",
        "type": 1,
        "name": "王五",
        "profession": "经理"
      },
      {
        "id": "node456",
        "type": 1,
        "name": "李四",
        "profession": "设计师"
      }
    ],
    "relations": [
      {
        "id": "rel123",
        "source": "node123",
        "target": "node789",
        "type": 1,
        "label": "同事"
      },
      {
        "id": "rel456",
        "source": "node789",
        "target": "node456",
        "type": 3,
        "label": "朋友"
      }
    ]
  }
  ```

## 6. 项目实现细节

### 6.1 项目结构

```
labelwall/
├── api/
│   └── protos/
│       └── relationship-network.thrift   # 服务接口定义 (IDL)
├── biz/                                  # 业务逻辑核心目录
│   ├── handler/                          # API 处理层: 存放 Hertz HTTP Handler，负责解析请求、参数校验、调用 Service 层并构造响应。
│   │   └── relationship/
│   │       └── network/
│   │           └── network_service.go    # NetworkService 对应的 Handler 实现
│   ├── model/                            # 模型层: 存放由 IDL 生成的 Go 数据结构 (struct, enum)，用于数据传输。
│   │   └── relationship/
│   │       └── network/
│   │           └── relationship-network.go # 由 Thrift IDL 生成的模型代码
│   ├── router/                           # 路由层: 定义 Hertz HTTP 路由规则，将 URL 路径映射到具体的 Handler 函数。
│   │   └── relationship/
│   │       └── network/
│   │           └── register.go           # NetworkService 相关路由的注册
│   ├── repo/                             # 数据访问层 (DAL/Repository): 负责与数据存储交互。
│   │   └── neo4jrepo/                    # Neo4j 存储的具体实现
│   │       ├── node_repo.go              # 节点相关的数据库操作 (CRUD, Search, etc.)
│   │       └── relation_repo.go          # 关系相关的数据库操作 (CRUD, Get by Node, etc.)
│   └── service/                          # 服务层: 封装核心业务逻辑，编排对 Repo 层的调用，处理复杂业务规则。
│       └── network_service.go            # NetworkService 接口定义及业务逻辑实现
├── cmd/
│   └── main.go                           # 程序入口: 初始化应用（配置、数据库、路由等），启动 Hertz 服务。
├── config/
│   └── config.yaml                       # 配置文件: 存放数据库连接信息、服务器端口等配置。
├── infrastructure/                       # 基础设施层: 存放与外部系统或底层服务交互的通用代码。
│   └── database/                         # 数据库相关基础设施
│       ├── connection.go                 # 定义数据库配置结构体，可能包含连接池管理逻辑。
│       └── init.go                       # 数据库初始化逻辑：创建连接、检查连通性、应用 Schema（索引、约束）。
├── pkg/                                  # 公共库/工具包
│   ├── constants/                        # 常量定义 (如错误码、默认值等)
│   ├── errors/                           # 自定义错误类型
│   └── util/                             # 通用工具函数
├── build.sh                              # 构建脚本: 用于编译 Go 程序，生成可执行文件。
├── go.mod                                # Go 模块定义文件
├── go.sum                                # Go 模块依赖校验和
└── README.md                             # 项目说明文档 (本文件)
```

### 6.2 数据存储实现

LabelWall 使用 Neo4j 图数据库存储关系网络数据。图数据库的特性非常适合存储和查询复杂的关系网络，支持高效的路径查找和网络遍历。

节点和关系在 Neo4j 中的存储结构示例：

```cypher
// 创建节点
CREATE (n:Person {id: "node123", name: "张三", profession: "工程师", avatar: "http://example.com/avatar.jpg"})

// 创建关系
MATCH (a:Person {id: "node123"}), (b:Person {id: "node456"})
CREATE (a)-[r:COLLEAGUE {id: "rel123", label: "同事", since: "2020"}]->(b)
```

### 6.3 API 实现详解

每个 API 端点都由对应的处理函数实现，大致流程如下：

1. 请求参数解析和验证
2. 调用数据访问层执行操作
3. 处理结果并构造响应

示例处理函数：

```go
// 创建节点处理函数
func CreateNode(ctx context.Context, c *app.RequestContext) {
    var req network.CreateNodeRequest
    err := c.BindAndValidate(&req)
    if err != nil {
        c.JSON(consts.StatusBadRequest, &network.CreateNodeResponse{
            Success: false,
            Message: "无效请求: " + err.Error(),
        })
        return
    }

    // 调用节点仓库创建节点
    node, err := nodeRepo.CreateNode(ctx, &req)
    if err != nil {
        c.JSON(consts.StatusInternalServerError, &network.CreateNodeResponse{
            Success: false,
            Message: "创建节点失败: " + err.Error(),
        })
        return
    }

    c.JSON(consts.StatusOK, &network.CreateNodeResponse{
        Success: true,
        Message: "节点创建成功",
        Node:    node,
    })
}
```

### 6.4 性能优化

- **查询缓存**: 使用 Redis 缓存热门查询结果，减轻数据库负担
- **分页查询**: 所有列表查询均支持分页，避免返回过大数据集
- **索引优化**: 对 Neo4j 中的节点属性建立适当索引，提高查询效率
- **连接池管理**: 优化数据库连接池配置，确保高并发下的性能

## 7. 部署指南

### 7.1 环境要求

- Go 1.16+
- Neo4j 4.0+
- Redis (可选，用于缓存)

### 7.2 配置说明

配置文件位于 `config/config.yaml`，主要配置项：

```yaml
server:
  port: 8888
  mode: release  # debug或release

database:
  neo4j:
    uri: bolt://localhost:7687
    username: neo4j
    password: password
    max_connections: 50
  
  redis:
    enabled: true
    addr: localhost:6379
    password: ""
    db: 0

logging:
  level: info  # debug, info, warn, error
  format: json  # text或json
  output: stdout  # stdout或文件路径
```

### 7.3 部署步骤

1. **安装依赖**
   ```bash
   go mod download
   ```

2. **编译项目**
   ```bash
   ./build.sh
   ```

3. **启动服务**
   ```bash
   ./labelwall -config config/config.yaml
   ```

### 7.4 Docker 部署

提供 Dockerfile 和 docker-compose.yml 实现容器化部署：

```dockerfile
FROM golang:1.18-alpine AS builder
WORKDIR /app
COPY . .
RUN go mod download
RUN go build -o labelwall cmd/main.go

FROM alpine:latest
WORKDIR /app
COPY --from=builder /app/labelwall .
COPY config/config.yaml ./config/
EXPOSE 8888
CMD ["./labelwall", "-config", "config/config.yaml"]
```

## 8. 开发指南

### 8.1 开发环境设置

1. **安装Go和开发工具**
   ```bash
   # 安装最新版Go
   wget https://golang.org/dl/go1.18.linux-amd64.tar.gz
   tar -C /usr/local -xzf go1.18.linux-amd64.tar.gz
   
   # 安装Hertz工具
   go install github.com/cloudwego/hertz/cmd/hz@latest
   ```

2. **安装Neo4j**
   ```bash
   # 使用Docker安装Neo4j
   docker run -p 7474:7474 -p 7687:7687 -e NEO4J_AUTH=neo4j/password neo4j:4.4
   ```

### 8.2 更新Thrift IDL

如果需要修改或添加API，请更新 `api/protos/relationship-network.thrift` 文件，然后运行以下命令重新生成代码：

```bash
hz update -idl api/protos/relationship-network.thrift
```

### 8.3 编写新API处理函数

1. 编辑生成的处理函数文件
2. 实现业务逻辑
3. 测试新API

### 8.4 测试

```bash
# 运行单元测试
go test ./...

# 使用curl测试API
curl -X POST http://localhost:8888/api/v1/nodes -d '{
  "type": 1,
  "name": "测试用户",
  "profession": "开发者"
}'
```

## 9. 使用示例

### 9.1 创建用户节点并建立关系

```bash
# 创建第一个节点
curl -X POST http://localhost:8888/api/v1/nodes \
  -H "Content-Type: application/json" \
  -d '{
    "type": 1,
    "name": "张三",
    "profession": "前端开发",
    "avatar": "http://example.com/zhangsan.jpg"
  }'

# 创建第二个节点
curl -X POST http://localhost:8888/api/v1/nodes \
  -H "Content-Type: application/json" \
  -d '{
    "type": 1,
    "name": "李四",
    "profession": "后端开发",
    "avatar": "http://example.com/lisi.jpg"
  }'

# 建立同事关系
curl -X POST http://localhost:8888/api/v1/relations \
  -H "Content-Type: application/json" \
  -d '{
    "source": "节点1ID",
    "target": "节点2ID",
    "type": 1,
    "label": "同事",
    "properties": {
      "company": "技术公司",
      "since": "2021"
    }
  }'
```

### 9.2 查询职业网络

```bash
# 查询所有前端开发相关网络
curl "http://localhost:8888/api/v1/network?profession=前端开发&depth=2"
```

### 9.3 查找两人之间的关系路径

```bash
# 查询张三和王五之间的关系路径
curl "http://localhost:8888/api/v1/path?source_id=张三ID&target_id=王五ID&max_depth=3"
```

## 10. 总结

LabelWall 关系网络服务提供了一个完整的解决方案，用于管理和查询复杂的社交和组织关系网络。通过结合 Hertz 框架的高性能特性和 Neo4j 图数据库的关系查询能力，该服务能够高效处理各种关系网络场景下的需求。

该服务的设计遵循了 RESTful API 原则，提供了直观的接口，便于前端开发者集成和使用。模块化的架构设计也使得系统易于扩展和维护。

未来，该服务可以进一步扩展，增加更多高级功能，如推荐系统、社区检测、关系预测等，为用户提供更丰富的关系网络分析能力。

---

## 附录：API 快速参考

| 功能 | 方法 | 端点 | 描述 |
|-----|-----|------|-----|
| **节点管理** | | | |
| 创建节点 | POST | /api/v1/nodes | 创建新节点 |
| 获取节点 | GET | /api/v1/nodes/:id | 获取节点详情 |
| 更新节点 | PUT | /api/v1/nodes/:id | 更新节点信息 |
| 删除节点 | DELETE | /api/v1/nodes/:id | 删除节点 |
| 搜索节点 | GET | /api/v1/nodes/search | 搜索节点 |
| **关系管理** | | | |
| 创建关系 | POST | /api/v1/relations | 创建节点关系 |
| 获取关系 | GET | /api/v1/relations/:id | 获取关系详情 |
| 更新关系 | PUT | /api/v1/relations/:id | 更新关系信息 |
| 删除关系 | DELETE | /api/v1/relations/:id | 删除关系 |
| 获取节点关系 | GET | /api/v1/nodes/:node_id/relations | 获取节点所有关系 |
| **网络查询** | | | |
| 网络查询 | GET | /api/v1/network | 按职业查询关系网络 |
| 路径查询 | GET | /api/v1/path | 查询节点间关系路径 |
