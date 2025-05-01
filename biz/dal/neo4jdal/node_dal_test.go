package neo4jdal

import (
	"context"
	"errors"
	"testing"
	"time"

	network "labelwall/biz/model/relationship/network"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/dbtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// MockSession 用于模拟 neo4j.SessionWithContext 接口
// 只模拟 ExecuteWrite 和 ExecuteRead 方法，直接返回预设值
type MockSession struct {
	mock.Mock
	neo4j.SessionWithContext // 嵌入接口
}

// ExecuteWrite 模拟写事务
func (m *MockSession) ExecuteWrite(ctx context.Context, work neo4j.ManagedTransactionWork, configurers ...func(*neo4j.TransactionConfig)) (any, error) {
	args := m.Called(ctx, work, configurers)
	return args.Get(0), args.Error(1)
}

// ExecuteRead 模拟读事务
func (m *MockSession) ExecuteRead(ctx context.Context, work neo4j.ManagedTransactionWork, configurers ...func(*neo4j.TransactionConfig)) (any, error) {
	args := m.Called(ctx, work, configurers)
	return args.Get(0), args.Error(1)
}

// --- 测试 ExecCreateNode ---
func TestNeo4jNodeDAL_ExecCreateNode(t *testing.T) {
	dal := NewNodeDAL()
	ctx := context.Background()

	nodeType := network.NodeType_PERSON
	properties := map[string]any{"id": "node1", "name": "Alice", "created_at": time.Now()}

	// 模拟创建成功时返回的节点对象
	mockNode := dbtype.Node{Id: 1, Labels: []string{"PERSON"}, Props: properties}

	t.Run("创建节点成功", func(t *testing.T) {
		mockSession := new(MockSession)
		// stub ExecuteWrite 返回 mockNode
		mockSession.On("ExecuteWrite", ctx, mock.AnythingOfType("neo4j.ManagedTransactionWork"), mock.Anything).Return(mockNode, nil).Once()

		node, err := dal.ExecCreateNode(ctx, mockSession, nodeType, properties)
		assert.NoError(t, err)
		assert.Equal(t, mockNode, node)
		mockSession.AssertExpectations(t)
	})

	t.Run("创建节点失败", func(t *testing.T) {
		mockSession := new(MockSession)
		expectedErr := errors.New("写事务失败")
		mockSession.On("ExecuteWrite", ctx, mock.AnythingOfType("neo4j.ManagedTransactionWork"), mock.Anything).Return(nil, expectedErr).Once()

		node, err := dal.ExecCreateNode(ctx, mockSession, nodeType, properties)
		assert.Error(t, err)
		assert.Equal(t, expectedErr, err)
		// 出错时返回零值节点
		assert.Equal(t, dbtype.Node{}, node)
		mockSession.AssertExpectations(t)
	})
}

// --- 测试 ExecGetNodeByID ---
func TestNeo4jNodeDAL_ExecGetNodeByID(t *testing.T) {
	dal := NewNodeDAL()
	ctx := context.Background()
	testID := "node123"

	// 模拟数据库返回的节点及标签
	dbNode := dbtype.Node{Id: 100, Labels: []string{"PERSON"}, Props: map[string]any{"id": testID}}
	labels := []string{"PERSON"}

	t.Run("查询到节点", func(t *testing.T) {
		mockSession := new(MockSession)
		// stub ExecuteRead 返回 map 表示找到
		mockSession.On("ExecuteRead", ctx, mock.AnythingOfType("neo4j.ManagedTransactionWork"), mock.Anything).
			Return(map[string]any{"node": dbNode, "labels": labels}, nil).Once()

		node, gotLabels, err := dal.ExecGetNodeByID(ctx, mockSession, testID)
		assert.NoError(t, err)
		assert.Equal(t, dbNode, node)
		assert.Equal(t, labels, gotLabels)
		mockSession.AssertExpectations(t)
	})

	t.Run("未找到节点", func(t *testing.T) {
		mockSession := new(MockSession)
		// ExecuteRead 返回 nil,nil 表示未找到
		mockSession.On("ExecuteRead", ctx, mock.AnythingOfType("neo4j.ManagedTransactionWork"), mock.Anything).
			Return(nil, nil).Once()

		node, gotLabels, err := dal.ExecGetNodeByID(ctx, mockSession, testID)
		assert.NoError(t, err)
		assert.Equal(t, dbtype.Node{}, node)
		assert.Nil(t, gotLabels)
		mockSession.AssertExpectations(t)
	})

	t.Run("读事务错误", func(t *testing.T) {
		mockSession := new(MockSession)
		expectedErr := errors.New("读事务失败")
		mockSession.On("ExecuteRead", ctx, mock.AnythingOfType("neo4j.ManagedTransactionWork"), mock.Anything).
			Return(nil, expectedErr).Once()

		node, gotLabels, err := dal.ExecGetNodeByID(ctx, mockSession, testID)
		assert.Error(t, err)
		assert.Equal(t, expectedErr, err)
		assert.Equal(t, dbtype.Node{}, node)
		assert.Nil(t, gotLabels)
		mockSession.AssertExpectations(t)
	})
}

// --- 测试 ExecUpdateNode ---
func TestNeo4jNodeDAL_ExecUpdateNode(t *testing.T) {
	dal := NewNodeDAL()
	ctx := context.Background()
	id := "node1"
	updates := map[string]any{"name": "Bob"}

	// 模拟更新成功返回的节点和标签
	dbNode := dbtype.Node{Id: 2, Labels: []string{"PERSON"}, Props: map[string]any{"id": id, "name": "Bob"}}
	labels := []string{"PERSON"}
	// stub ExecuteWrite 返回 map[string]any
	mockSession := new(MockSession)
	mockSession.On("ExecuteWrite", ctx, mock.AnythingOfType("neo4j.ManagedTransactionWork"), mock.Anything).
		Return(map[string]any{"node": dbNode, "labels": labels}, nil).Once()

	node, gotLabels, err := dal.ExecUpdateNode(ctx, mockSession, id, updates)
	assert.NoError(t, err)
	assert.Equal(t, dbNode, node)
	assert.Equal(t, labels, gotLabels)
	mockSession.AssertExpectations(t)
}

// --- 测试 ExecDeleteNode ---
func TestNeo4jNodeDAL_ExecDeleteNode(t *testing.T) {
	dal := NewNodeDAL()
	ctx := context.Background()
	id := "node1"

	t.Run("写事务失败", func(t *testing.T) {
		mockSession := new(MockSession)
		expectedErr := errors.New("delete failed")
		mockSession.On("ExecuteWrite", ctx, mock.AnythingOfType("neo4j.ManagedTransactionWork"), mock.Anything).
			Return(nil, expectedErr).Once()

		err := dal.ExecDeleteNode(ctx, mockSession, id)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), expectedErr.Error())
		mockSession.AssertExpectations(t)
	})

	t.Run("返回非预期类型", func(t *testing.T) {
		mockSession := new(MockSession)
		// stub 返回非 ResultSummary 类型
		mockSession.On("ExecuteWrite", ctx, mock.AnythingOfType("neo4j.ManagedTransactionWork"), mock.Anything).
			Return("not_summary", nil).Once()

		err := dal.ExecDeleteNode(ctx, mockSession, id)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "非预期的结果类型")
		mockSession.AssertExpectations(t)
	})
}

// --- 测试 ExecSearchNodes ---
func TestNeo4jNodeDAL_ExecSearchNodes(t *testing.T) {
	dal := NewNodeDAL()
	ctx := context.Background()
	criteria := map[string]string{"name": "Al"}
	var nodeType *network.NodeType = nil
	limit, offset := int64(5), int64(0)

	// 模拟搜索结果
	n1 := dbtype.Node{Id: 10, Labels: []string{"PERSON"}, Props: map[string]any{"name": "Alice"}}
	nodes := []neo4j.Node{n1}
	labelsList := [][]string{{"PERSON"}}
	total := int64(1)

	mockSession := new(MockSession)
	mockSession.On("ExecuteRead", ctx, mock.AnythingOfType("neo4j.ManagedTransactionWork"), mock.Anything).
		Return(map[string]any{"nodes": nodes, "labels": labelsList, "total": total}, nil).Once()

	gotNodes, gotLabels, gotTotal, err := dal.ExecSearchNodes(ctx, mockSession, criteria, nodeType, limit, offset)
	assert.NoError(t, err)
	assert.Equal(t, nodes, gotNodes)
	assert.Equal(t, labelsList, gotLabels)
	assert.Equal(t, total, gotTotal)
	mockSession.AssertExpectations(t)
}

// --- 测试 ExecGetNetwork ---
func TestNeo4jNodeDAL_ExecGetNetwork(t *testing.T) {
	dal := NewNodeDAL()
	ctx := context.Background()
	profession := "Engineer"
	depth := int32(2)
	limit, offset := int64(3), int64(1)

	// 模拟网络查询结果
	n0 := dbtype.Node{Id: 100, Labels: []string{"PERSON"}, Props: map[string]any{"profession": profession}}
	r0 := dbtype.Relationship{Id: 200, Type: "COLLEAGUE"}
	nodes := []neo4j.Node{n0}
	rels := []neo4j.Relationship{r0}

	mockSession := new(MockSession)
	mockSession.On("ExecuteRead", ctx, mock.AnythingOfType("neo4j.ManagedTransactionWork"), mock.Anything).
		Return(map[string]any{"nodes": nodes, "rels": rels}, nil).Once()

	gotNodes, gotRels, err := dal.ExecGetNetwork(ctx, mockSession, profession, depth, limit, offset)
	assert.NoError(t, err)
	assert.Equal(t, nodes, gotNodes)
	assert.Equal(t, rels, gotRels)
	mockSession.AssertExpectations(t)
}

// --- 测试 ExecGetPath ---
func TestNeo4jNodeDAL_ExecGetPath(t *testing.T) {
	dal := NewNodeDAL()
	ctx := context.Background()
	src, dst := "A", "B"
	maxDepth := int32(1)
	relTypes := []string{"FRIEND"}

	// 模拟路径查询结果
	n1 := dbtype.Node{Id: 1, Labels: []string{"PERSON"}, Props: map[string]any{"id": src}}
	n2 := dbtype.Node{Id: 2, Labels: []string{"PERSON"}, Props: map[string]any{"id": dst}}
	nodes := []neo4j.Node{n1, n2}
	r1 := dbtype.Relationship{Id: 300, Type: "FRIEND"}
	rels := []neo4j.Relationship{r1}

	mockSession := new(MockSession)
	mockSession.On("ExecuteRead", ctx, mock.AnythingOfType("neo4j.ManagedTransactionWork"), mock.Anything).
		Return(map[string]any{"nodes": nodes, "rels": rels}, nil).Once()

	gotNodes, gotRels, err := dal.ExecGetPath(ctx, mockSession, src, dst, maxDepth, relTypes)
	assert.NoError(t, err)
	assert.Equal(t, nodes, gotNodes)
	assert.Equal(t, rels, gotRels)
	mockSession.AssertExpectations(t)
}
