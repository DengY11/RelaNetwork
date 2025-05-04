namespace go relationship.network

// 节点类型
enum NodeType {
    PERSON = 1
    COMPANY = 2
    SCHOOL = 3
    // 可添加更多节点类型
}

// 关系类型
enum RelationType {
    COLLEAGUE = 1     // 同事
    SCHOOLMATE = 2    // 校友
    FRIEND = 3        // 朋友
    VISITED = 4       // 访问过
    FOLLOWING = 5     // 关注
    // 可添加更多关系类型
}

// 节点信息
struct Node {
    1: string id              // 节点ID
    2: NodeType type          // 节点类型
    3: string name            // 名称
    4: optional string avatar // 头像URL
    5: optional string profession // 职业
    6: optional map<string, string> properties // 其他属性
}

// 关系信息
struct Relation {
    1: string id              // 关系ID
    2: string source          // 起始节点ID
    3: string target          // 目标节点ID
    4: RelationType type      // 关系类型
    5: optional string label  // 关系标签
    6: optional map<string, string> properties // 关系属性
}

// =============== 节点 CRUD 操作 ===============

// 创建节点请求
struct CreateNodeRequest {
    1: NodeType type
    2: string name
    3: optional string avatar
    4: optional string profession
    5: optional map<string, string> properties
}

// 创建节点响应
struct CreateNodeResponse {
    1: bool success
    2: string message
    3: Node node
}

// 更新节点请求
struct UpdateNodeRequest {
    1: string id              // 要更新的节点ID
    2: optional string name
    3: optional string avatar
    4: optional string profession
    5: optional map<string, string> properties
}

// 更新节点响应
struct UpdateNodeResponse {
    1: bool success
    2: string message
    3: Node node
}

// 获取节点请求
struct GetNodeRequest {
    1: string id
}

// 获取节点响应
struct GetNodeResponse {
    1: bool success
    2: string message
    3: Node node
}

// 删除节点请求
struct DeleteNodeRequest {
    1: string id
}

// 删除节点响应
struct DeleteNodeResponse {
    1: bool success
    2: string message
}

// 搜索节点请求
struct SearchNodesRequest {
    1: optional map<string, string> criteria // 新增: 搜索条件 (key: 属性名, value: 搜索值)
    2: optional NodeType type   // 节点类型(可选)
    3: optional i32 limit       // 限制返回数量
    4: optional i32 offset      // 偏移量，用于分页
}

// 搜索节点响应
struct SearchNodesResponse {
    1: bool success
    2: string message
    3: list<Node> nodes
    4: i32 total               // 总匹配数
}

// =============== 关系 CRUD 操作 ===============

// 创建关系请求
struct CreateRelationRequest {
    1: string source           // 源节点ID
    2: string target           // 目标节点ID
    3: RelationType type       // 关系类型
    4: optional string label   // 关系标签
    5: optional map<string, string> properties // 关系属性
}

// 创建关系响应
struct CreateRelationResponse {
    1: bool success
    2: string message
    3: Relation relation
}

// 更新关系请求
struct UpdateRelationRequest {
    1: string id               // 关系ID
    2: optional RelationType type
    3: optional string label
    4: optional map<string, string> properties
}

// 更新关系响应
struct UpdateRelationResponse {
    1: bool success
    2: string message
    3: Relation relation
}

// 获取关系请求
struct GetRelationRequest {
    1: string id
}

// 获取关系响应
struct GetRelationResponse {
    1: bool success
    2: string message
    3: Relation relation
}

// 删除关系请求
struct DeleteRelationRequest {
    1: string id
}

// 删除关系响应
struct DeleteRelationResponse {
    1: bool success
    2: string message
}

// 获取节点关系请求
struct GetNodeRelationsRequest {
    1: string node_id          // 节点ID
    2: optional list<RelationType> types // 关系类型筛选(可选)
    3: optional bool outgoing  // 是否包含出去的关系，默认true
    4: optional bool incoming  // 是否包含进来的关系，默认true
    5: optional i32 limit      // 限制返回数量
    6: optional i32 offset     // 偏移量，用于分页
}

// 获取节点关系响应
struct GetNodeRelationsResponse {
    1: bool success
    2: string message
    3: list<Relation> relations
    4: i32 total              // 总关系数
}

// 网络查询请求
struct GetNetworkRequest {
    1: string profession         // 按职业筛选
    2: optional i32 depth        // 查询深度
}

// 网络查询响应
struct GetNetworkResponse {
    1: bool success
    2: string message
    3: list<Node> nodes          // 返回完整节点信息而非ID
    4: list<Relation> relations
}

// 路径查询请求
struct GetPathRequest {
    1: string source_id          // 起始节点ID
    2: string target_id          // 目标节点ID
    3: optional i32 max_depth    // 最大查询深度
    4: optional list<RelationType> types // 关系类型筛选(可选)
}

// 路径查询响应
struct GetPathResponse {
    1: bool success
    2: string message
    3: list<Node> nodes          // 路径上的节点
    4: list<Relation> relations  // 路径上的关系
}

// 关系网络服务定义
service NetworkService {
    // 网络查询
    GetNetworkResponse GetNetwork(1: GetNetworkRequest req) (api.get="/api/v1/network")

    // 路径查询
    GetPathResponse GetPath(1: GetPathRequest req) (api.get="/api/v1/path")

    // 搜索节点
    SearchNodesResponse SearchNodes(1: SearchNodesRequest req) (api.get="/api/v1/nodes/search")

    // 节点 CRUD
    CreateNodeResponse CreateNode(1: CreateNodeRequest req) (api.post="/api/v1/nodes")
    GetNodeResponse GetNode(1: GetNodeRequest req) (api.get="/api/v1/nodes/:id")
    UpdateNodeResponse UpdateNode(1: UpdateNodeRequest req) (api.put="/api/v1/nodes/:id")
    DeleteNodeResponse DeleteNode(1: DeleteNodeRequest req) (api.delete="/api/v1/nodes/:id")

    // 关系 CRUD
    CreateRelationResponse CreateRelation(1: CreateRelationRequest req) (api.post="/api/v1/relations")
    GetRelationResponse GetRelation(1: GetRelationRequest req) (api.get="/api/v1/relations/:id")
    UpdateRelationResponse UpdateRelation(1: UpdateRelationRequest req) (api.put="/api/v1/relations/:id")
    DeleteRelationResponse DeleteRelation(1: DeleteRelationRequest req) (api.delete="/api/v1/relations/:id")

    // 获取节点的所有关系
    GetNodeRelationsResponse GetNodeRelations(1: GetNodeRelationsRequest req) (api.get="/api/v1/nodes/:node_id/relations")
}
