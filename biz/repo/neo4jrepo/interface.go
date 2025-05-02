package neo4jrepo

import (
	"context"

	network "labelwall/biz/model/relationship/network"
)

// NodeRepository 定义了节点数据访问的操作接口。
// Repo 层负责处理业务逻辑 ID、与 DAL 交互、模型转换以及可能的缓存逻辑。
type NodeRepository interface {
	// CreateNode 创建一个新节点。
	// 输入：CreateNodeRequest 包含创建节点所需的信息。
	// 输出：创建成功的节点对象或错误。
	CreateNode(ctx context.Context, req *network.CreateNodeRequest) (*network.Node, error)

	// GetNode 根据 ID 获取节点信息。
	// 输入：节点 ID。
	// 输出：找到的节点对象或错误（例如，未找到）。
	GetNode(ctx context.Context, id string) (*network.Node, error)

	// UpdateNode 更新一个现有节点。
	// 输入：UpdateNodeRequest 包含节点 ID 和要更新的字段。
	// 输出：更新后的节点对象或错误（例如，未找到）。
	UpdateNode(ctx context.Context, req *network.UpdateNodeRequest) (*network.Node, error)

	// DeleteNode 根据 ID 删除节点。
	// 输入：节点 ID。
	// 输出：错误（例如，未找到或删除失败）。
	DeleteNode(ctx context.Context, id string) error

	// SearchNodes 根据条件搜索节点。
	// 输入：SearchNodesRequest 包含搜索关键字、类型、分页等信息。
	// 输出：匹配的节点列表、符合条件的总数以及错误。
	SearchNodes(ctx context.Context, req *network.SearchNodesRequest) ([]*network.Node, int32, error)

	// GetNetwork 查询指定职业相关的网络图谱。
	// 输入：GetNetworkRequest 包含职业、查询深度、分页等信息。
	// 输出：网络中的节点列表、关系列表以及错误。
	GetNetwork(ctx context.Context, req *network.GetNetworkRequest) ([]*network.Node, []*network.Relation, error)

	// GetPath 查询两个节点之间的最短路径。
	// 输入：GetPathRequest 包含起始节点 ID、目标节点 ID、最大深度、关系类型过滤等。
	// 输出：路径上的节点列表、关系列表以及错误（例如，路径未找到）。
	GetPath(ctx context.Context, req *network.GetPathRequest) ([]*network.Node, []*network.Relation, error)
}

// RelationRepository 定义了关系数据访问的操作接口。
type RelationRepository interface {
	// CreateRelation 创建一个新关系。
	// 输入：CreateRelationRequest 包含源/目标节点 ID、关系类型和属性。
	// 输出：创建成功的关系对象或错误（例如，节点不存在）。
	CreateRelation(ctx context.Context, req *network.CreateRelationRequest) (*network.Relation, error)

	// GetRelation 根据 ID 获取关系信息。
	// 输入：关系 ID。
	// 输出：找到的关系对象或错误（例如，未找到）。
	GetRelation(ctx context.Context, id string) (*network.Relation, error)

	// UpdateRelation 更新一个现有关系。
	// 输入：UpdateRelationRequest 包含关系 ID 和要更新的字段。
	// 输出：更新后的关系对象或错误（例如，未找到）。
	UpdateRelation(ctx context.Context, req *network.UpdateRelationRequest) (*network.Relation, error)

	// DeleteRelation 根据 ID 删除关系。
	// 输入：关系 ID。
	// 输出：错误（例如，未找到或删除失败）。
	DeleteRelation(ctx context.Context, id string) error

	// GetNodeRelations 获取指定节点的所有（或部分）关系。
	// 输入：GetNodeRelationsRequest 包含节点 ID、关系类型过滤、方向、分页等信息。
	// 输出：匹配的关系列表、符合条件的总数以及错误。
	GetNodeRelations(ctx context.Context, req *network.GetNodeRelationsRequest) ([]*network.Relation, int32, error)
}
