package neo4jrepo

import (
	"errors"
	"fmt"
	network "labelwall/biz/model/relationship/network"
	"strings"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j/dbtype"
)

// Define repository-level error for path not found HERE
var ErrPathNotFound = errors.New("repo: path not found")

// --- 通用辅助函数 ---

// isNotFoundError 检查错误是否表示"未找到"
// TODO:这个地方需要鲁棒性更强的实现
func isNotFoundError(err error) bool {
	if err == nil {
		return false
	}

	if errors.Is(err, ErrPathNotFound) {
		return true
	}

	lowerCaseErr := strings.ToLower(err.Error())
	if strings.Contains(lowerCaseErr, "not found") {
		return true
	}
	if strings.Contains(lowerCaseErr, "could not find node") {
		return true
	}
	if strings.Contains(lowerCaseErr, "could not find relationship") {
		return true
	}
	// Add check for the specific error message from path finding
	if strings.Contains(lowerCaseErr, "result contains no more records") { // Specific check for path finding
		return true
	}
	// Add other specific 'not found' messages if needed

	return false
}

// labelToNodeType 从 Neo4j 标签列表推断 Thrift NodeType
func labelToNodeType(labels []string) (network.NodeType, bool) {
	for _, label := range labels {
		// 使用 Thrift 生成的 FromString 函数进行查找
		if nodeType, err := network.NodeTypeFromString(label); err == nil {
			return nodeType, true
		}
	}
	return network.NodeType(0), false // 未找到匹配的标签
}

// stringToRelationType 从 Neo4j 关系类型字符串推断 Thrift RelationType
func stringToRelationType(relTypeStr string) (network.RelationType, bool) {
	if relType, err := network.RelationTypeFromString(relTypeStr); err == nil {
		return relType, true
	}
	return network.RelationType(0), false
}

// mapDbNodeToThriftNode 将 Neo4j 节点对象转换为 Thrift Node 对象
func mapDbNodeToThriftNode(dbNode dbtype.Node, nodeType network.NodeType) *network.Node {
	props := dbNode.Props
	node := &network.Node{
		Type:       nodeType,
		ID:         getStringProp(props, "id", ""),
		Name:       getStringProp(props, "name", ""),
		Avatar:     getOptionalStringProp(props, "avatar"),
		Profession: getOptionalStringProp(props, "profession"),
		Properties: make(map[string]string),
	}

	coreProps := map[string]struct{}{ // 核心和通用字段
		"id": {}, "name": {}, "avatar": {}, "profession": {}, "created_at": {}, "updated_at": {},
	}
	for key, val := range props {
		if _, isCore := coreProps[key]; !isCore {
			if strVal, ok := val.(string); ok {
				node.Properties[key] = strVal
			}
		}
	}
	if len(node.Properties) == 0 {
		node.Properties = nil
	}

	return node
}

// mapDbRelationshipToThriftRelation 将 Neo4j 关系对象转换为 Thrift Relation 对象
func mapDbRelationshipToThriftRelation(dbRel any, relType network.RelationType, sourceID, targetID string) *network.Relation {
	var props map[string]any

	switch r := dbRel.(type) {
	case dbtype.Relationship: // 保留这个更通用的类型
		props = r.Props
	default:
		fmt.Printf("WARN: mapDbRelationshipToThriftRelation 收到未知类型: %T\n", dbRel)
		return nil
	}

	relation := &network.Relation{
		Type:       relType,
		Source:     sourceID,
		Target:     targetID,
		ID:         getStringProp(props, "id", ""),
		Label:      getOptionalStringProp(props, "label"),
		Properties: make(map[string]string),
	}

	coreProps := map[string]struct{}{ // 核心和通用字段
		"id": {}, "label": {}, "created_at": {}, "updated_at": {},
	}
	for key, val := range props {
		if _, isCore := coreProps[key]; !isCore {
			if strVal, ok := val.(string); ok {
				relation.Properties[key] = strVal
			}
		}
	}
	if len(relation.Properties) == 0 {
		relation.Properties = nil
	}

	return relation
}

func getStringProp(props map[string]any, key string, defaultValue string) string {
	if props == nil {
		return defaultValue
	}
	if val, ok := props[key].(string); ok {
		return val
	}
	return defaultValue
}

func getOptionalStringProp(props map[string]any, key string) *string {
	if props == nil {
		return nil
	}
	if val, ok := props[key].(string); ok {
		if val != "" {
			return &val
		}
	}
	return nil
}
