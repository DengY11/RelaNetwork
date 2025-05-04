package service

import (
	"context"
	"errors" // Import errors package
	"fmt"

	"labelwall/biz/dal/neo4jdal" // Import for ErrNotFound
	network "labelwall/biz/model/relationship/network"
	neo4jrepo "labelwall/biz/repo/neo4jrepo" // 导入数据访问层
	"labelwall/pkg/cache"                    // Import for cache errors
)

// Helper function to check for various "not found" errors
func isNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, neo4jdal.ErrNotFound) ||
		errors.Is(err, cache.ErrNotFound) ||
		errors.Is(err, cache.ErrNilValue) // Treat cached nil as not found at service level
}

// NetworkService 定义了关系网络服务的业务逻辑接口
// 这些方法对应 Thrift service 中的定义
type NetworkService interface {
	// 网络查询
	GetNetwork(ctx context.Context, req *network.GetNetworkRequest) (*network.GetNetworkResponse, error)
	// 路径查询
	GetPath(ctx context.Context, req *network.GetPathRequest) (*network.GetPathResponse, error)
	// 搜索节点
	SearchNodes(ctx context.Context, req *network.SearchNodesRequest) (*network.SearchNodesResponse, error)
	// 节点 CRUD
	CreateNode(ctx context.Context, req *network.CreateNodeRequest) (*network.CreateNodeResponse, error)
	GetNode(ctx context.Context, req *network.GetNodeRequest) (*network.GetNodeResponse, error)
	UpdateNode(ctx context.Context, req *network.UpdateNodeRequest) (*network.UpdateNodeResponse, error)
	DeleteNode(ctx context.Context, req *network.DeleteNodeRequest) (*network.DeleteNodeResponse, error)
	// 关系 CRUD
	CreateRelation(ctx context.Context, req *network.CreateRelationRequest) (*network.CreateRelationResponse, error)
	GetRelation(ctx context.Context, req *network.GetRelationRequest) (*network.GetRelationResponse, error)
	UpdateRelation(ctx context.Context, req *network.UpdateRelationRequest) (*network.UpdateRelationResponse, error)
	DeleteRelation(ctx context.Context, req *network.DeleteRelationRequest) (*network.DeleteRelationResponse, error)
	// 获取节点的所有关系
	GetNodeRelations(ctx context.Context, req *network.GetNodeRelationsRequest) (*network.GetNodeRelationsResponse, error)
}

type networkService struct {
	nodeRepo     neo4jrepo.NodeRepository
	relationRepo neo4jrepo.RelationRepository
}

func NewNetworkService(nodeRepo neo4jrepo.NodeRepository, relationRepo neo4jrepo.RelationRepository) NetworkService {
	return &networkService{
		nodeRepo:     nodeRepo,
		relationRepo: relationRepo,
	}
}

// CreateNode 处理创建节点的业务逻辑
func (s *networkService) CreateNode(ctx context.Context, req *network.CreateNodeRequest) (*network.CreateNodeResponse, error) {
	// 1. 输入验证 (可以在 handler 层做，也可以在 service 层补充)
	if req.Name == "" {
		return &network.CreateNodeResponse{Success: false, Message: "节点名称不能为空"}, nil
	}

	// 2. 调用 repo 层创建节点
	node, err := s.nodeRepo.CreateNode(ctx, req)
	if err != nil {
		// 实际应用中应记录错误日志
		fmt.Printf("Service: CreateNode 失败: %v\n", err)
		return &network.CreateNodeResponse{Success: false, Message: fmt.Sprintf("创建节点失败: %v", err)}, nil // 可以考虑返回更通用的错误信息
	}

	// 3. 构建成功响应
	return &network.CreateNodeResponse{
		Success: true,
		Message: "节点创建成功",
		Node:    node,
	}, nil
}

// GetNode 处理获取节点的业务逻辑
func (s *networkService) GetNode(ctx context.Context, req *network.GetNodeRequest) (*network.GetNodeResponse, error) {
	node, err := s.nodeRepo.GetNode(ctx, req.ID)
	if err != nil {
		if isNotFoundError(err) {
			return &network.GetNodeResponse{Success: false, Message: fmt.Sprintf("节点未找到: ID=%s", req.ID)}, nil
		}
		// 其他错误
		fmt.Printf("Service: GetNode 失败 (ID: %s): %v\n", req.ID, err)
		return nil, fmt.Errorf("获取节点失败: %w", err) // 返回包装后的错误，让上层处理
	}

	// 成功找到
	return &network.GetNodeResponse{
		Success: true,
		Message: "节点获取成功",
		Node:    node,
	}, nil
}

// UpdateNode 处理更新节点的业务逻辑
func (s *networkService) UpdateNode(ctx context.Context, req *network.UpdateNodeRequest) (*network.UpdateNodeResponse, error) {
	node, err := s.nodeRepo.UpdateNode(ctx, req)
	if err != nil {
		if isNotFoundError(err) {
			return &network.UpdateNodeResponse{Success: false, Message: fmt.Sprintf("要更新的节点未找到: ID=%s", req.ID)}, nil
		}
		// 其他错误
		fmt.Printf("Service: UpdateNode 失败 (ID: %s): %v\n", req.ID, err)
		return nil, fmt.Errorf("更新节点失败: %w", err)
	}

	// 成功更新
	return &network.UpdateNodeResponse{
		Success: true,
		Message: "节点更新成功",
		Node:    node,
	}, nil
}

// DeleteNode 处理删除节点的业务逻辑
func (s *networkService) DeleteNode(ctx context.Context, req *network.DeleteNodeRequest) (*network.DeleteNodeResponse, error) {
	err := s.nodeRepo.DeleteNode(ctx, req.ID)
	if err != nil {
		if isNotFoundError(err) {
			// 即使 Repo 返回 NotFound，从 Service 角度看，目标节点最终不存在，操作可视为"成功完成"
			return &network.DeleteNodeResponse{Success: true, Message: fmt.Sprintf("节点不存在或已被删除: ID=%s", req.ID)}, nil
		}
		// 其他错误
		fmt.Printf("Service: DeleteNode 失败 (ID: %s): %v\n", req.ID, err)
		return nil, fmt.Errorf("删除节点失败: %w", err)
	}

	// 成功删除 (或原本就不存在)
	return &network.DeleteNodeResponse{
		Success: true,
		Message: "节点删除成功",
	}, nil
}

// CreateRelation 处理创建关系的业务逻辑
func (s *networkService) CreateRelation(ctx context.Context, req *network.CreateRelationRequest) (*network.CreateRelationResponse, error) {
	// 1. 输入验证
	if req.Source == "" || req.Target == "" {
		return &network.CreateRelationResponse{Success: false, Message: "源节点和目标节点 ID 不能为空"}, nil
	}

	// 2. 调用 repo 层创建关系
	relation, err := s.relationRepo.CreateRelation(ctx, req)
	if err != nil {
		fmt.Printf("Service: CreateRelation 失败: %v\n", err)
		// 考虑处理特定错误，例如节点不存在
		return &network.CreateRelationResponse{Success: false, Message: fmt.Sprintf("创建关系失败: %v", err)}, nil
	}

	// 3. 构建成功响应
	return &network.CreateRelationResponse{
		Success:  true,
		Message:  "关系创建成功",
		Relation: relation,
	}, nil
}

// GetRelation 处理获取关系的业务逻辑
func (s *networkService) GetRelation(ctx context.Context, req *network.GetRelationRequest) (*network.GetRelationResponse, error) {
	relation, err := s.relationRepo.GetRelation(ctx, req.ID)
	if err != nil {
		if isNotFoundError(err) {
			return &network.GetRelationResponse{Success: false, Message: fmt.Sprintf("关系未找到: ID=%s", req.ID)}, nil
		}
		// 其他错误
		fmt.Printf("Service: GetRelation 失败 (ID: %s): %v\n", req.ID, err)
		return nil, fmt.Errorf("获取关系失败: %w", err)
	}

	// 成功找到
	return &network.GetRelationResponse{
		Success:  true,
		Message:  "关系获取成功",
		Relation: relation,
	}, nil
}

// UpdateRelation 处理更新关系的业务逻辑
func (s *networkService) UpdateRelation(ctx context.Context, req *network.UpdateRelationRequest) (*network.UpdateRelationResponse, error) {
	relation, err := s.relationRepo.UpdateRelation(ctx, req)
	if err != nil {
		if isNotFoundError(err) {
			return &network.UpdateRelationResponse{Success: false, Message: fmt.Sprintf("要更新的关系未找到: ID=%s", req.ID)}, nil
		}
		// 其他错误
		fmt.Printf("Service: UpdateRelation 失败 (ID: %s): %v\n", req.ID, err)
		return nil, fmt.Errorf("更新关系失败: %w", err)
	}

	// 成功更新
	return &network.UpdateRelationResponse{
		Success:  true,
		Message:  "关系更新成功",
		Relation: relation,
	}, nil
}

// DeleteRelation 处理删除关系的业务逻辑
func (s *networkService) DeleteRelation(ctx context.Context, req *network.DeleteRelationRequest) (*network.DeleteRelationResponse, error) {
	err := s.relationRepo.DeleteRelation(ctx, req.ID)
	if err != nil {
		if isNotFoundError(err) {
			return &network.DeleteRelationResponse{Success: true, Message: fmt.Sprintf("关系不存在或已被删除: ID=%s", req.ID)}, nil
		}
		// 其他错误
		fmt.Printf("Service: DeleteRelation 失败 (ID: %s): %v\n", req.ID, err)
		return nil, fmt.Errorf("删除关系失败: %w", err)
	}

	// 成功删除
	return &network.DeleteRelationResponse{
		Success: true,
		Message: "关系删除成功",
	}, nil
}

// SearchNodes 处理搜索节点的业务逻辑
func (s *networkService) SearchNodes(ctx context.Context, req *network.SearchNodesRequest) (*network.SearchNodesResponse, error) {
	nodes, total, err := s.nodeRepo.SearchNodes(ctx, req)
	if err != nil {
		// 搜索失败通常不认为是致命错误，除非是底层连接问题
		fmt.Printf("Service: SearchNodes 失败 (Criteria: %v): %v\n", req.Criteria, err)
		// 可以选择返回空结果或错误
		return nil, fmt.Errorf("搜索节点失败: %w", err) // 返回错误，让上层决定如何响应
	}

	// 即使找不到结果 (len(nodes) == 0)，也视为成功执行了搜索
	return &network.SearchNodesResponse{
		Success: true,
		Message: fmt.Sprintf("搜索完成，找到 %d 个节点", total),
		Nodes:   nodes,
		Total:   total,
	}, nil
}

// GetNodeRelations 处理获取节点关系的业务逻辑
func (s *networkService) GetNodeRelations(ctx context.Context, req *network.GetNodeRelationsRequest) (*network.GetNodeRelationsResponse, error) {
	relations, total, err := s.relationRepo.GetNodeRelations(ctx, req)
	if err != nil {
		fmt.Printf("Service: GetNodeRelations 失败 (NodeID: %s): %v\n", req.NodeID, err)
		return nil, fmt.Errorf("获取节点关系失败: %w", err)
	}

	return &network.GetNodeRelationsResponse{
		Success:   true,
		Message:   fmt.Sprintf("获取关系完成，找到 %d 个关系", total),
		Relations: relations,
		Total:     total,
	}, nil
}

// GetNetwork 处理网络查询的业务逻辑
func (s *networkService) GetNetwork(ctx context.Context, req *network.GetNetworkRequest) (*network.GetNetworkResponse, error) {
	nodes, relations, err := s.nodeRepo.GetNetwork(ctx, req)
	if err != nil {
		fmt.Printf("Service: GetNetwork 失败 (StartCriteria: %v): %v\n", req.StartNodeCriteria, err)
		return nil, fmt.Errorf("获取网络图谱失败: %w", err)
	}

	// GetNetwork 找不到匹配通常返回空列表，不视为错误
	return &network.GetNetworkResponse{
		Success:   true,
		Message:   fmt.Sprintf("获取网络图谱完成，找到 %d 个节点，%d 条关系", len(nodes), len(relations)),
		Nodes:     nodes,
		Relations: relations,
	}, nil
}

// GetPath 处理路径查询的业务逻辑
func (s *networkService) GetPath(ctx context.Context, req *network.GetPathRequest) (*network.GetPathResponse, error) {
	nodes, relations, err := s.nodeRepo.GetPath(ctx, req)
	if err != nil {
		// GetPath 对于路径不存在会返回错误，我们需要检查这种特定情况
		// 使用 isNotFoundError，因为它应该能捕捉到 Repo 层包装的 Path Not Found 错误
		if isNotFoundError(err) {
			return &network.GetPathResponse{Success: false, Message: fmt.Sprintf("未找到从 %s 到 %s 的路径", req.SourceID, req.TargetID)}, nil
		}
		// 其他错误
		fmt.Printf("Service: GetPath 失败 (Source: %s, Target: %s): %v\n", req.SourceID, req.TargetID, err)
		return nil, fmt.Errorf("查询路径失败: %w", err)
	}

	// 成功找到路径
	return &network.GetPathResponse{
		Success:   true,
		Message:   "路径查询成功",
		Nodes:     nodes,
		Relations: relations,
	}, nil
}
