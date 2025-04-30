package neo4jdal

import (
	"context"
	network "labelwall/biz/model/relationship/network"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/dbtype"
)

// NodeDAL 定义了节点数据访问的底层操作
type NodeDAL interface {
	ExecCreateNode(ctx context.Context, session neo4j.SessionWithContext, nodeType network.NodeType, properties map[string]any) (neo4j.Node, error)
	ExecGetNodeByID(ctx context.Context, session neo4j.SessionWithContext, id string) (neo4j.Node, []string /*labels*/, error)
	ExecUpdateNode(ctx context.Context, session neo4j.SessionWithContext, id string, updates map[string]any) (neo4j.Node, []string /*labels*/, error)
	ExecDeleteNode(ctx context.Context, session neo4j.SessionWithContext, id string) error
	ExecSearchNodes(ctx context.Context, session neo4j.SessionWithContext, keyword string, nodeType *network.NodeType, limit, offset int64) ([]neo4j.Node, [][]string /*labels*/, int64 /*total*/, error)
	ExecGetNetwork(ctx context.Context, session neo4j.SessionWithContext, profession string, depth int32) ([]neo4j.Node, []neo4j.Relationship, error)
	ExecGetPath(ctx context.Context, session neo4j.SessionWithContext, sourceID, targetID string, maxDepth int32, relTypes []string) ([]neo4j.Node, []neo4j.Relationship, error)
}

// RelationDAL 定义了关系数据访问的底层操作
type RelationDAL interface {
	ExecCreateRelation(ctx context.Context, session neo4j.SessionWithContext, sourceID, targetID string, relType network.RelationType, properties map[string]any) (neo4j.Relationship, error)
	ExecGetRelationByID(ctx context.Context, session neo4j.SessionWithContext, id string) (dbtype.Relationship, string /*type*/, string /*sourceId*/, string /*targetId*/, error)
	ExecUpdateRelation(ctx context.Context, session neo4j.SessionWithContext, id string, updates map[string]any) (dbtype.Relationship, string /*type*/, string /*sourceId*/, string /*targetId*/, error)
	ExecDeleteRelation(ctx context.Context, session neo4j.SessionWithContext, id string) error
	ExecGetNodeRelations(ctx context.Context, session neo4j.SessionWithContext, nodeID string, types []string, outgoing, incoming bool, limit, offset int64) ([]dbtype.Relationship, []string /*types*/, []string /*sourceIds*/, []string /*targetIds*/, int64 /*total*/, error)
}
