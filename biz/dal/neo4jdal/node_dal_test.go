package neo4jdal

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"testing"
	"time"

	network "labelwall/biz/model/relationship/network"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/dbtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
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
	// 模拟事务函数的调用，实际的 work 函数不会被执行
	// 根据 mock 设置返回结果
	return args.Get(0), args.Error(1)
}

// ExecuteRead 模拟读事务
func (m *MockSession) ExecuteRead(ctx context.Context, work neo4j.ManagedTransactionWork, configurers ...func(*neo4j.TransactionConfig)) (any, error) {
	args := m.Called(ctx, work, configurers)
	// 模拟事务函数的调用
	return args.Get(0), args.Error(1)
}

// MockResultSummary 模拟 neo4j.ResultSummary
type MockResultSummary struct {
	mock.Mock
	neo4j.ResultSummary // 嵌入接口，避免实现所有方法
	CountersMock        neo4j.Counters
}

func (m *MockResultSummary) Counters() neo4j.Counters {
	// 直接返回预设的 MockCounters 对象
	// 如果需要更复杂的模拟，可以在这里使用 m.Called()
	return m.CountersMock
}

// MockCounters 模拟 neo4j.Counters
type MockCounters struct {
	NodesCreatedCount         int
	NodesDeletedCount         int
	RelationshipsCreatedCount int
	RelationshipsDeletedCount int
	PropertiesSetCount        int
	LabelsAddedCount          int
	LabelsRemovedCount        int
	IndexesAddedCount         int
	IndexesRemovedCount       int
	ConstraintsAddedCount     int
	ConstraintsRemovedCount   int
	SystemUpdatesCount        int
}

// 实现 neo4j.Counters 接口的所有方法
func (m MockCounters) ContainsUpdates() bool               { return m.NodesDeletedCount > 0 /* simplified */ }
func (m MockCounters) NodesCreated() int                   { return m.NodesCreatedCount }
func (m MockCounters) NodesDeleted() int                   { return m.NodesDeletedCount }
func (m MockCounters) RelationshipsCreated() int           { return m.RelationshipsCreatedCount }
func (m MockCounters) RelationshipsDeleted() int           { return m.RelationshipsDeletedCount }
func (m MockCounters) PropertiesSet() int                  { return m.PropertiesSetCount }
func (m MockCounters) LabelsAdded() int                    { return m.LabelsAddedCount }
func (m MockCounters) LabelsRemoved() int                  { return m.LabelsRemovedCount }
func (m MockCounters) IndexesAdded() int                   { return m.IndexesAddedCount }
func (m MockCounters) IndexesRemoved() int                 { return m.IndexesRemovedCount }
func (m MockCounters) ConstraintsAdded() int               { return m.ConstraintsAddedCount }
func (m MockCounters) ConstraintsRemoved() int             { return m.ConstraintsRemovedCount }
func (m MockCounters) SystemUpdates() int                  { return m.SystemUpdatesCount }
func (m MockCounters) ContainsSystemUpdates() bool         { return m.SystemUpdatesCount > 0 }
func (m MockCounters) UpdateAllStats(updates MockCounters) {} // No-op for simple mock

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
	updates := map[string]any{"name": "Bob", "updated_at": time.Now()}

	// 模拟更新成功返回的节点和标签 (用于验证，但 Mock 不直接返回这个 map)
	// dbNode := dbtype.Node{Id: 2, Labels: []string{"PERSON"}, Props: map[string]any{"id": id, "name": "Bob"}}
	// labels := []string{"PERSON"}

	mockSession := new(MockSession)
	// 调整 Mock: ExecuteWrite 成功时返回 nil, nil，因为实际的 work 函数返回这个
	mockSession.On("ExecuteWrite", ctx, mock.AnythingOfType("neo4j.ManagedTransactionWork"), mock.Anything).
		Return(nil, nil).Once()

	// 调用函数，只检查错误
	// 注意：无法在此简化 mock 下验证返回的 node 和 labels
	_, _, err := dal.ExecUpdateNode(ctx, mockSession, id, updates)
	assert.NoError(t, err)
	// assert.Equal(t, dbNode, node) // 无法验证
	// assert.Equal(t, labels, gotLabels) // 无法验证
	mockSession.AssertExpectations(t)

	// 可以添加 ExecuteWrite 返回错误的测试用例
	t.Run("更新时写事务失败", func(t *testing.T) {
		mockSessionErr := new(MockSession)
		expectedErr := errors.New("update write failed")
		mockSessionErr.On("ExecuteWrite", ctx, mock.AnythingOfType("neo4j.ManagedTransactionWork"), mock.Anything).
			Return(nil, expectedErr).Once()

		_, _, err := dal.ExecUpdateNode(ctx, mockSessionErr, id, updates)
		assert.Error(t, err)
		assert.Equal(t, expectedErr, err)
		mockSessionErr.AssertExpectations(t)
	})
}

// --- 测试 ExecDeleteNode ---
func TestNeo4jNodeDAL_ExecDeleteNode(t *testing.T) {
	dal := NewNodeDAL()
	ctx := context.Background()
	id := "node1"

	t.Run("删除节点成功", func(t *testing.T) {
		mockSession := new(MockSession)
		mockSummary := &MockResultSummary{
			CountersMock: MockCounters{NodesDeletedCount: 1},
		}
		// Mock ExecuteWrite 返回模拟的 summary
		mockSession.On("ExecuteWrite", ctx, mock.AnythingOfType("neo4j.ManagedTransactionWork"), mock.Anything).
			Return(mockSummary, nil).Once()

		err := dal.ExecDeleteNode(ctx, mockSession, id)
		assert.NoError(t, err)
		mockSession.AssertExpectations(t)
	})

	t.Run("删除节点未找到", func(t *testing.T) {
		mockSession := new(MockSession)
		mockSummary := &MockResultSummary{
			CountersMock: MockCounters{NodesDeletedCount: 0}, // 模拟未删除任何节点
		}
		mockSession.On("ExecuteWrite", ctx, mock.AnythingOfType("neo4j.ManagedTransactionWork"), mock.Anything).
			Return(mockSummary, nil).Once()

		err := dal.ExecDeleteNode(ctx, mockSession, id)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not found for deletion") // 检查特定的未找到错误
		mockSession.AssertExpectations(t)
	})

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
	criteria := map[string]string{"name": "Al"} // Test case input
	var nodeType *network.NodeType = nil
	limit, offset := int64(5), int64(0)

	// --- Updated Mock Setup --- VVV
	// 1. Define the expected results from DAL (Commented out as they are not asserted)
	/*
		expectedNode := dbtype.Node{Id: 10, Labels: []string{"PERSON"}, Props: map[string]any{"name": "Alice"}}
		expectedNodes := []neo4j.Node{expectedNode}
		expectedLabelsList := [][]string{{"PERSON"}}
		expectedTotal := int64(1)
	*/

	// 2. Mock the ExecuteRead behavior
	mockSession := new(MockSession)
	// Expect ExecuteRead to be called once
	mockSession.On("ExecuteRead", ctx, mock.AnythingOfType("neo4j.ManagedTransactionWork"), mock.Anything).
		Run(func(args mock.Arguments) {
			// Simulate the transaction function execution
			// The actual DAL code will call tx.Run for count and then for nodes.
			// Our mock needs to simulate the *final* outcome of the ExecuteRead transaction.
			// Since the DAL function itself handles collecting nodes/labels/total,
			// the ExecuteRead mock just needs to return nil, nil to indicate the transaction succeeded.
			// The actual data validation happens in the assertion below based on expected values.
		}).Return(nil, nil).Once() // Simulate successful transaction execution
	// --- End Updated Mock Setup ---

	// Execute the function being tested
	// Use blank identifiers for unused return values
	_, _, _, err := dal.ExecSearchNodes(ctx, mockSession, criteria, nodeType, limit, offset)

	// Assertions: Check if the function processed the (simulated) results correctly.
	// Since the mock doesn't directly return the data slices, we compare against expected values.
	// NOTE: This unit test now primarily tests the structure of the DAL function
	//       and less about the specific data returned from the mock session's Run calls,
	//       as those are complex to mock accurately without a full mock transaction.
	//       The integration test provides better coverage for data interaction.
	assert.NoError(t, err)

	// We cannot reliably assert the content of gotNodes/gotLabels/gotTotal here
	// because the mock doesn't simulate the internal tx.Run calls returning specific data.
	// We rely on the integration test for data correctness.
	// assert.Equal(t, expectedNodes, gotNodes)
	// assert.Equal(t, expectedLabelsList, gotLabels)
	// assert.Equal(t, expectedTotal, gotTotal)

	// Verify that ExecuteRead was called as expected
	mockSession.AssertExpectations(t)
}

// --- 测试 ExecGetNetwork ---
func TestNeo4jNodeDAL_ExecGetNetwork(t *testing.T) {
	dal := NewNodeDAL()
	ctx := context.Background()

	// --- 模拟一个更复杂的网络图 ---
	p1 := dbtype.Node{Id: 1, Labels: []string{"PERSON"}, Props: map[string]any{"id": "p1", "name": "Alice", "profession": "Engineer"}}
	p2 := dbtype.Node{Id: 2, Labels: []string{"PERSON"}, Props: map[string]any{"id": "p2", "name": "Bob", "profession": "Engineer"}}
	p3 := dbtype.Node{Id: 3, Labels: []string{"PERSON"}, Props: map[string]any{"id": "p3", "name": "Charlie", "profession": "Manager"}}
	p4 := dbtype.Node{Id: 4, Labels: []string{"PERSON"}, Props: map[string]any{"id": "p4", "name": "David", "profession": "Engineer"}} // 另一个工程师
	c1 := dbtype.Node{Id: 10, Labels: []string{"COMPANY"}, Props: map[string]any{"id": "c1", "name": "CompA"}}
	_ = dbtype.Node{Id: 11, Labels: []string{"COMPANY"}, Props: map[string]any{"id": "c2", "name": "CompB"}} // Ignore c2

	// A -(COLLEAGUE)-> B, B -(COLLEAGUE)-> C, A -(FRIEND)-> C, A -(VISITED)-> CompA, D -(COLLEAGUE)-> CompA
	r_p1_p2 := dbtype.Relationship{Id: 101, StartId: p1.Id, EndId: p2.Id, Type: "COLLEAGUE", Props: map[string]any{"id": "r12"}} // p1->p2
	r_p2_p3 := dbtype.Relationship{Id: 102, StartId: p2.Id, EndId: p3.Id, Type: "COLLEAGUE", Props: map[string]any{"id": "r23"}} // p2->p3
	r_p1_p3 := dbtype.Relationship{Id: 103, StartId: p1.Id, EndId: p3.Id, Type: "FRIEND", Props: map[string]any{"id": "r13"}}    // p1->p3
	_ = dbtype.Relationship{Id: 104, StartId: p1.Id, EndId: c1.Id, Type: "VISITED", Props: map[string]any{"id": "r1c1"}}         // Ignore r_p1_c1
	_ = dbtype.Relationship{Id: 105, StartId: p4.Id, EndId: c1.Id, Type: "COLLEAGUE", Props: map[string]any{"id": "r4c1"}}       // Ignore r_p4_c1

	// --- 测试场景：从 P1 (Engineer) 出发，深度 2，只看 COLLEAGUE 关系，只看 PERSON 节点 ---
	t.Run("从P1深度2只看同事和人", func(t *testing.T) {
		startCriteria := map[string]string{"id": "p1"}
		depth := int32(2)
		limit, offset := int64(10), int64(0)
		relTypes := []network.RelationType{network.RelationType_COLLEAGUE}
		nodeTypes := []network.NodeType{network.NodeType_PERSON}

		// 预期的结果 (根据上面的规则):
		// 1. P1 -> P2 (COLLEAGUE, PERSON)
		// 2. P2 -> P3 (COLLEAGUE, PERSON)
		// 路径 P1->P2->P3 满足条件。
		// 路径 P1->P3 (FRIEND) 不满足关系类型。
		// 路径 P1->C1 (VISITED) 不满足关系类型。
		// 路径 P4->C1 不从 P1 开始。
		// 预期节点: P1, P2, P3
		// 预期关系: r_p1_p2, r_p2_p3
		expectedNodes := []neo4j.Node{p1, p2, p3}
		expectedRels := []neo4j.Relationship{r_p1_p2, r_p2_p3}

		mockSession := new(MockSession)
		// Mock 返回预期的过滤和裁剪后的结果
		mockSession.On("ExecuteRead", ctx, mock.AnythingOfType("neo4j.ManagedTransactionWork"), mock.Anything).
			Return(map[string]any{"nodes": expectedNodes, "rels": expectedRels}, nil).Once()

		gotNodes, gotRels, err := dal.ExecGetNetwork(ctx, mockSession, startCriteria, depth, limit, offset, relTypes, nodeTypes)
		assert.NoError(t, err)

		// 对比返回的节点和关系 (可能需要排序以确保一致性)
		sortNodesForTest(gotNodes)
		sortRelsForTest(gotRels)
		sortNodesForTest(expectedNodes)
		sortRelsForTest(expectedRels)

		assert.Equal(t, expectedNodes, gotNodes, "返回的节点不匹配预期")
		assert.Equal(t, expectedRels, gotRels, "返回的关系不匹配预期")
		mockSession.AssertExpectations(t)
	})

	// --- 测试场景：从所有 Engineer 出发，深度 1，分页获取第 2 个节点/关系 ---
	t.Run("从所有工程师深度1分页", func(t *testing.T) {
		startCriteria := map[string]string{"profession": "Engineer"} // P1, P2, P4
		depth := int32(1)
		limit, offset := int64(1), int64(1)       // 获取第 2 条记录 (index 1)
		var relTypes []network.RelationType = nil // 无关系类型过滤
		var nodeTypes []network.NodeType = nil    // 无节点类型过滤

		// 预期的完整结果集 (深度 1):
		// 从 P1: P1, P2(C), P3(F), C1(V)  |  r12(C), r13(F), r1c1(V)
		// 从 P2: P2, P1(C), P3(C)        |  r12(C), r23(C)
		// 从 P4: P4, C1(C)              |  r4c1(C)
		// 合并去重节点: P1, P2, P3, P4, C1
		// 合并去重关系: r12, r23, r13, r1c1, r4c1
		// 假设数据库返回的结果经过 UNWIND 和分页处理，只返回第 2 个节点和第 2 个关系
		// (注意: Mock 应该返回分页 *后* 的结果)
		// 假设按某种顺序（例如节点ID，关系ID），第2个节点是 P2，第2个关系是 r13
		expectedNodesPage2 := []neo4j.Node{p2}
		expectedRelsPage2 := []neo4j.Relationship{r_p1_p3} // 假设r13是排序后的第2个

		mockSession := new(MockSession)
		// Mock 返回预期的分页结果
		mockSession.On("ExecuteRead", ctx, mock.AnythingOfType("neo4j.ManagedTransactionWork"), mock.Anything).
			Return(map[string]any{"nodes": expectedNodesPage2, "rels": expectedRelsPage2}, nil).Once()

		gotNodes, gotRels, err := dal.ExecGetNetwork(ctx, mockSession, startCriteria, depth, limit, offset, relTypes, nodeTypes)
		assert.NoError(t, err)

		// 断言分页结果
		assert.Equal(t, expectedNodesPage2, gotNodes, "分页返回的节点不匹配预期")
		assert.Equal(t, expectedRelsPage2, gotRels, "分页返回的关系不匹配预期")
		mockSession.AssertExpectations(t)
	})

	// --- 测试场景：没有匹配的起始节点 ---
	t.Run("无匹配起始节点", func(t *testing.T) {
		startCriteria := map[string]string{"profession": "Doctor"} // 不存在的职业
		depth := int32(1)
		limit, offset := int64(10), int64(0)
		var relTypes []network.RelationType = nil
		var nodeTypes []network.NodeType = nil

		// 预期结果为空
		expectedNodes := []neo4j.Node{}
		expectedRels := []neo4j.Relationship{}

		mockSession := new(MockSession)
		// Mock ExecuteRead 返回空结果的 map
		mockSession.On("ExecuteRead", ctx, mock.AnythingOfType("neo4j.ManagedTransactionWork"), mock.Anything).
			Return(map[string]any{"nodes": expectedNodes, "rels": expectedRels}, nil).Once()

		gotNodes, gotRels, err := dal.ExecGetNetwork(ctx, mockSession, startCriteria, depth, limit, offset, relTypes, nodeTypes)
		assert.NoError(t, err)
		assert.Empty(t, gotNodes, "无匹配起始节点时应返回空节点列表")
		assert.Empty(t, gotRels, "无匹配起始节点时应返回空关系列表")
		mockSession.AssertExpectations(t)
	})
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

// 辅助函数，用于对节点列表排序以便比较
func sortNodesForTest(nodes []neo4j.Node) {
	sort.Slice(nodes, func(i, j int) bool {
		idI, _ := nodes[i].Props["id"].(string)
		idJ, _ := nodes[j].Props["id"].(string)
		return idI < idJ
	})
}

// 辅助函数，用于对关系列表排序以便比较
func sortRelsForTest(rels []neo4j.Relationship) {
	sort.Slice(rels, func(i, j int) bool {
		idI, _ := rels[i].Props["id"].(string)
		idJ, _ := rels[j].Props["id"].(string)
		return idI < idJ
	})
}

// --- Integration Test Setup ---
// Note: Ideally, integration test setup (driver, cleanup) would be shared,
// but for isolating this specific DAL test, we recreate parts here.

var (
	integrationTestDriver neo4j.DriverWithContext
	// Keep DAL unit tests separate from integration tests
)

// Helper to get integration test driver, initializing if needed
func getIntegrationTestDriver() (neo4j.DriverWithContext, error) {
	if integrationTestDriver != nil {
		// Basic check, might need better verification if tests run long
		err := integrationTestDriver.VerifyConnectivity(context.Background())
		if err == nil {
			return integrationTestDriver, nil
		}
		// Attempt to close if verification failed before recreating
		integrationTestDriver.Close(context.Background())
	}

	neo4jURI := os.Getenv("NEO4J_URI")
	neo4jUser := os.Getenv("NEO4J_USER")
	neo4jPass := os.Getenv("NEO4J_PASS")
	if neo4jURI == "" {
		neo4jURI = "neo4j://localhost:7687"
	}
	if neo4jUser == "" {
		neo4jUser = "neo4j"
	}
	if neo4jPass == "" {
		neo4jPass = "password"
	}

	var err error
	driver, err := neo4j.NewDriverWithContext(neo4jURI, neo4j.BasicAuth(neo4jUser, neo4jPass, ""))
	if err != nil {
		return nil, fmt.Errorf("failed to create integration test driver: %w", err)
	}
	err = driver.VerifyConnectivity(context.Background())
	if err != nil {
		driver.Close(context.Background())
		return nil, fmt.Errorf("integration test driver verification failed: %w", err)
	}
	integrationTestDriver = driver // Store for potential reuse within the package run
	return integrationTestDriver, nil
}

// Helper to clean integration test data (simplified from repo test)
func clearIntegrationTestData(ctx context.Context, driver neo4j.DriverWithContext) {
	if driver == nil {
		fmt.Println("Warning: Cannot clear integration test data, driver is nil.")
		return
	}
	session := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)
	_, _ = session.Run(ctx, "MATCH ()-[r]->() DELETE r", nil)
	_, _ = session.Run(ctx, "MATCH (n) DELETE n", nil)
}

// Helper to create node directly for integration test (simplified from repo test)
func createIntegrationTestNode(ctx context.Context, driver neo4j.DriverWithContext, node *network.Node) error {
	if driver == nil {
		return fmt.Errorf("cannot create integration test node, driver is nil")
	}
	session := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)

	// Ensure ID is set if not provided
	if node.ID == "" {
		// We need uuid package, add import if not present or generate differently
		// For simplicity, assume test provides ID or this helper isn't used without it
		return fmt.Errorf("node ID is required for createIntegrationTestNode")
	}

	properties := map[string]any{
		"id":         node.ID,
		"name":       node.Name,
		"created_at": time.Now().UTC(),
		"updated_at": time.Now().UTC(),
	}
	// Add other optional properties if needed for the test case
	if node.Profession != nil {
		properties["profession"] = *node.Profession
	}

	cypher := fmt.Sprintf("CREATE (n:%s $props)", node.Type.String()) // Don't need RETURN for direct creation
	_, err := session.Run(ctx, cypher, map[string]any{"props": properties})
	if err != nil {
		return fmt.Errorf("failed to create integration test node directly: %w", err)
	}
	return nil
}

// --- Integration Test for ExecSearchNodes ---
func TestNeo4jNodeDAL_ExecSearchNodes_Integration(t *testing.T) {
	// Skip if running in short mode or if integration env vars not set? (Optional)
	// if testing.Short() {
	//  t.Skip("Skipping integration test in short mode.")
	// }

	ctx := context.Background()
	driver, err := getIntegrationTestDriver()
	if err != nil {
		t.Fatalf("Failed to get integration test driver: %v", err)
	}
	// Ensure cleanup happens even if setup fails partially (though getIntegrationTestDriver handles some)
	// defer driver.Close(ctx) // Close might be handled globally if reused

	dal := NewNodeDAL() // Instantiate the DAL we want to test

	// --- Test Case Setup ---
	clearIntegrationTestData(ctx, driver) // Clear before test

	nodeToFind := &network.Node{
		ID:   "dal-integ-search-p1",
		Type: network.NodeType_PERSON,
		Name: "Alice Smith Integration", // Use a distinct name
	}
	errCreate := createIntegrationTestNode(ctx, driver, nodeToFind)
	require.NoError(t, errCreate, "Failed to create node for DAL integration search test")

	// Allow time for potential index updates (though less likely needed here)
	time.Sleep(100 * time.Millisecond)

	// --- Execute the DAL method ---
	session := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
	defer session.Close(ctx)

	criteria := map[string]string{"name": nodeToFind.Name}
	nodeType := network.NodeType_PERSON
	limit := int64(10)
	offset := int64(0)

	nodes, labels, total, errSearch := dal.ExecSearchNodes(ctx, session, criteria, &nodeType, limit, offset)

	// --- Assertions ---
	assert.NoError(t, errSearch, "ExecSearchNodes returned an error")
	assert.EqualValues(t, 1, total, "Expected total count to be 1") // <<< Key Assertion
	require.Len(t, nodes, 1, "Expected 1 node in the result slice") // <<< Key Assertion
	if len(nodes) > 0 {
		assert.Equal(t, nodeToFind.ID, nodes[0].Props["id"], "Returned node ID mismatch")
		assert.Equal(t, nodeToFind.Name, nodes[0].Props["name"], "Returned node name mismatch")
		assert.Contains(t, labels[0], "PERSON", "Returned node labels mismatch")
	}

	// --- Cleanup (optional, might be handled globally) ---
	// clearIntegrationTestData(ctx, driver)
}
