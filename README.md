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
- **Go**: 后端开发语言，版本 1.20+

### 2.2 数据存储

- **图数据库** (如 Neo4j): 用于存储和查询关系网络数据
- **Redis**: 用于缓存常用查询结果，提高读取性能，降低数据库负载

### 2.3 开发工具

- **hz**: Hertz 代码生成工具，用于从 IDL 生成服务代码
- **thriftgo**: Thrift IDL 编译器，生成 Thrift 相关代码

## 3. 系统架构

### 3.1 整体架构

```
+----------------+     +-------------------+     +------------------+
|                | GET +-->+  缓存层       +<--+ |                  |
|  客户端应用    |     |  (Redis)      |   | |  图数据库       |
|  (Web/Mobile)  +---->+  LabelWall API    +-->+ |  (Neo4j)         |
|                | POST|  (Hertz/Thrift)   |   | |                  |
+----------------+ PUT +-->+                   |   | +------------------+
                 DELETE|  +-------------------+   |
                       |   ^                     |
                       +---+---------------------+ (DB Query on Cache Miss)
```

**说明**:
- 客户端通过 Hertz/Thrift API 与后端服务交互。
- API 服务层处理请求，并优先尝试从 **缓存层 (Redis)** 获取数据。
- 如果缓存未命中，服务层将查询 **图数据库 (Neo4j)**。
- 查询结果（或表示数据不存在的标记）会被写回缓存层，供后续请求使用。
- 写操作（创建、更新、删除）会直接操作图数据库，并根据策略 **使相关缓存失效**。

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
- **描述**: 根据条件搜索节点
- **查询参数**:
    - `criteria` - 可选, 搜索条件map (e.g., `criteria[name]=张`, `criteria[profession]=工程师`)。后端将对提供的 value 进行模糊匹配或精确匹配（具体取决于实现）。键必须是节点属性名。
    - `type` - 可选，节点类型 (`1`=PERSON, `2`=COMPANY, `3`=SCHOOL)
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
    "total": 2 // 匹配到的总节点数
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

#### 5.3.1 网络查询

- **端点**: `GET /api/v1/network`
- **描述**: 根据起始节点条件查询相关节点及其关系网络
- **查询参数**:
    - `startNodeCriteria` - 可选, 用于查找起始节点的条件 map (e.g., `startNodeCriteria[profession]=工程师`, `startNodeCriteria[id]=node123`)。
    - `depth` - 可选，从起始节点扩展的查询深度，默认为1。`0` 表示只返回起始节点。负数无效。
    - `relationTypes` - 可选, 关系类型列表 (e.g., `1,3`)，用于过滤遍历的关系。
    - `nodeTypes` - 可选, 节点类型列表 (e.g., `1,2`)，用于过滤最终结果中的节点。
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
      },
      {
         "id": "node789",
         "type": 2,
         "name": "ABC公司"
      }
    ],
    "relations": [
      {
        "id": "rel123",
        "source": "node123",
        "target": "node456",
        "type": 1,
        "label": "同事"
      },
      {
        "id": "rel179",
        "source": "node123",
        "target": "node789",
        "type": 1,
        "label": "前同事"
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
    - `types` - 可选，关系类型列表 (e.g., `1,3`)，用于筛选路径中允许的关系类型。
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
relationship-network/
├── relationship-network.thrift       # Thrift IDL
├── generate_code.sh                  # Thrift 代码生成脚本
├── build.sh                          # 构建脚本
├── go.mod, go.sum                    # Go 模块依赖
├── main.go                           # 程序入口
├── router.go, router_gen.go          # 路由注册与生成
├── biz/                              # 业务逻辑核心目录
│   ├── handler/relationship/network/ # Hertz Handler 层
│   ├── model/relationship/network/   # Thrift 生成的模型代码
│   ├── repo/neo4jrepo/               # Repository 层实现
│   └── service/relationship/network/ # Service 层实现
├── infrastructure/                   # 基础设施目录
│   └── database/                     # 初始化与连接实现
├── script/                           # 脚本目录
├── output/                           # 产物输出目录
└── README.md                         # 项目说明文档
```

### 6.2 数据存储实现

relationship-network  使用 Neo4j 图数据库存储关系网络数据。图数据库的特性非常适合存储和查询复杂的关系网络，支持高效的路径查找和网络遍历。

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

- **索引优化**: 对 Neo4j 中的节点和关系的关键属性（如 `id`, `name`）建立索引，加速查找和匹配操作。
- **查询优化**: 编写高效的 Cypher 查询语句，避免全图扫描，利用索引和数据库特性。
- **分页查询**: 所有返回列表的 API（如 `SearchNodes`, `GetNetwork, GetPath, GetNodeRelations`）均支持分页参数 (`limit`, `offset`)，控制单次返回的数据量，减少网络传输和内存消耗。
- **连接池管理**: 配置合理的 Neo4j 驱动连接池大小，平衡并发请求和资源消耗。
- **缓存策略**: (见下文详细说明)

#### 6.4.1 缓存策略详解

为了提升读取性能并减少对图数据库的直接访问压力，本项目集成了基于 Redis 的缓存层。主要采用以下策略：

1.  **缓存存储**: 使用 Redis 存储缓存数据。
2.  **基本模式**: 
    *   **读操作 (GetNode, GetRelation)**: 采用 **读旁路 (Read-Aside)** 模式。优先查询 Redis 缓存，如果命中则直接返回；如果未命中，则查询 Neo4j，将结果写入 Redis 缓存（设置 TTL）后再返回给客户端。
    *   **写操作 (Create, Update, Delete)**: 采用 **写失效 (Write Invalidation)** 模式。当执行更新或删除操作时，直接操作 Neo4j 数据库，**然后删除 Redis 中对应的缓存条目**，确保下次读取时能获取最新数据。创建操作通常不直接写缓存，依赖首次读取填充。
3.  **复杂查询缓存 (SearchNodes, GetNetwork, GetPath, GetNodeRelations)**:
    *   这些查询的结果集可能较大且参数组合多样，直接缓存完整结果对象开销大且命中率低。
    *   采用**缓存 ID 列表 + 单实体缓存组合**的策略：
        *   缓存的 Key 包含所有查询参数（必要时进行哈希处理）。
        *   缓存的 Value 仅存储满足条件的**节点 ID 列表**和/或**关系 ID 列表**，以及**结果总数**（用于分页）。
        *   当缓存命中时，获取 ID 列表，然后利用已经缓存的 `GetNode` 和 `GetRelation` 方法（它们会走自己的缓存逻辑）获取每个实体对象的详细信息，最后组装成完整结果返回。
    *   这种方式减少了缓存冗余，并能更快地反映单个实体的更新。
4.  **缓存穿透与雪崩防治**:
    *   **缓存空值**: 对于查询数据库确认不存在的单个实体（如 `GetNode` 未找到），缓存一个特殊的 `nil` 标记（`NilValuePlaceholder`）并设置较短的 TTL（如 `NilValueTTL`），防止后续请求重复查询数据库。
    *   对于复杂查询（如 `SearchNodes`）返回空结果集的情况，也缓存一个特殊的空标记（如 `searchEmptyPlaceholder`）和较短 TTL。
    *   **TTL Jitter**: 在设置缓存的 TTL 时，增加一个小的随机扰动时间（基于 `DefaultTTLJitterPercent`），避免大量缓存在同一精确时间失效导致缓存雪崩。
5.  **缓存键设计**: 
    *   使用明确的前缀（如 `node:`, `relation:`, `search:nodes:ids:`, `network:graph:ids:`）区分不同类型的缓存。
    *   对于包含用户输入（如搜索关键字）或可变参数列表（如关系类型）的 Key，使用 SHA1 哈希处理，确保 Key 的格式规范且长度可控。

## 7. 部署指南

### 7.1 环境要求

- Go 1.20+
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

项目包含不同层级的测试，以确保代码质量和功能正确性：

- **单元测试 (Unit Tests)**:
    - 主要针对 DAL 层和部分 Service/Repo 层的纯逻辑单元。
    - 使用 `stretchr/testify/mock` 等库模拟依赖项（如数据库会话），隔离被测单元。
    - 运行快速，不依赖外部服务。
    - 执行命令: `go test ./biz/dal/...` (示例)

- **集成测试 (Integration Tests)**:
    - 主要针对 Repo 层和 Service 层。
    - 需要连接**真实的 Neo4j 和 Redis 实例**来验证数据交互和缓存逻辑。
    - 确保在运行前已启动并配置好 Neo4j 和 Redis (参照 7.1 环境要求 和 7.2 配置说明)。
    - 覆盖数据创建、读取、更新、删除 (CRUD) 以及缓存命中/失效等场景。
    - 执行命令: `go test ./biz/repo/neo4jrepo -v` 或 `go test ./biz/service -v` (示例)

- **端到端测试 (End-to-End Tests)** (可选):
    - 可以使用 `curl` 或其他 HTTP 客户端工具，直接调用运行中服务的 API 端点，验证完整流程。

**运行所有测试**:
```bash
# 运行项目下的所有测试 (包括单元和集成测试)
# 确保 Neo4j 和 Redis 正在运行并已配置
go test ./...
```

**使用 curl 进行基本 API 测试**:
```bash
# 示例：创建节点
curl -X POST http://localhost:8888/api/v1/nodes -d '{
  "type": 1,
  "name": "测试用户",
  "profession": "开发者"
}' -H "Content-Type: application/json"

# 示例：获取节点 (将 :id 替换为实际 ID)
curl http://localhost:8888/api/v1/nodes/:id
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
| 搜索节点 | GET | /api/v1/nodes/search | 按条件搜索节点 |
| **关系管理** | | | |
| 创建关系 | POST | /api/v1/relations | 创建节点关系 |
| 获取关系 | GET | /api/v1/relations/:id | 获取关系详情 |
| 更新关系 | PUT | /api/v1/relations/:id | 更新关系信息 |
| 删除关系 | DELETE | /api/v1/relations/:id | 删除关系 |
| 获取节点关系 | GET | /api/v1/nodes/:node_id/relations | 获取节点所有关系 |
| **网络查询** | | | |
| 网络查询 | GET | /api/v1/network | 按起始条件查询关系网络 |
| 路径查询 | GET | /api/v1/path | 查询节点间关系路径 |
