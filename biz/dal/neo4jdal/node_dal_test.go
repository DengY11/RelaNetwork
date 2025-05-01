package neo4jdal

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	network "labelwall/biz/model/relationship/network"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/dbtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// --- Mock Definitions ---

// MockSession mocks neo4j.SessionWithContext
type MockSession struct {
	mock.Mock
	neo4j.SessionWithContext // Embed the interface
}

// Override methods used in DAL
func (m *MockSession) ExecuteWrite(ctx context.Context, work neo4j.ManagedTransactionWork, configurers ...func(*neo4j.TransactionConfig)) (any, error) {
	args := m.Called(ctx, work, configurers)
	// To properly mock ExecuteWrite/ExecuteRead, we need to execute the provided 'work' function
	// with a mock transaction if the mocked call doesn't return an error directly.
	if args.Error(1) == nil {
		// Assuming the first return value (args.Get(0)) would be the result
		// if the transaction function itself wasn't called.
		// We create a mock transaction and pass it to the actual 'work' function.
		mockTx := new(MockTransaction)
		// Retrieve the pre-configured mock transaction setup if available
		// This allows the test to define what tx.Run should do.
		// A bit complex: the test needs to mock the transaction's behavior *before* mocking the session.ExecuteWrite call.
		// A simpler way might be to just pass mockTx and let the test setup expectations on mockTx directly.
		// Let's try the simpler way first. The test must set expectations on MockTransaction before this runs.

		// The 'work' function is called here. It will interact with mockTx.
		res, err := work(mockTx)
		// Check if 'work' returned an error
		if err != nil {
			return nil, err // Return the error from the 'work' function
		}
		// Return the result from the 'work' function and nil error (as configured in the mock expectation)
		return res, nil
	}
	// Return the mocked error directly without calling 'work'
	return args.Get(0), args.Error(1)
}

func (m *MockSession) ExecuteRead(ctx context.Context, work neo4j.ManagedTransactionWork, configurers ...func(*neo4j.TransactionConfig)) (any, error) {
	args := m.Called(ctx, work, configurers)
	if args.Error(1) == nil {
		mockTx := new(MockTransaction)
		// Allow the test to set expectations on this specific mockTx instance
		m.TestData().Set("mockTx", mockTx)
		res, err := work(mockTx)
		if err != nil {
			return nil, err
		}
		return res, nil
	}
	return args.Get(0), args.Error(1)
}

// MockTransaction mocks neo4j.ManagedTransaction
type MockTransaction struct {
	mock.Mock
	neo4j.ManagedTransaction // Embed the interface
}

// Override Run method
func (m *MockTransaction) Run(ctx context.Context, cypher string, params map[string]any) (neo4j.ResultWithContext, error) {
	args := m.Called(ctx, cypher, params)
	err := args.Error(1)
	if err != nil {
		return nil, err // 优先返回mock设置的错误
	}
	res := args.Get(0)
	if res == nil {
		// 如果mock未设置返回值且未设置错误，这是mock设置的问题
		return nil, fmt.Errorf("mock error: Run() called but no return value set in mock for query: %s", cypher)
	}
	return res.(neo4j.ResultWithContext), nil
}

// MockResult mocks neo4j.ResultWithContext
type MockResult struct {
	mock.Mock
	neo4j.ResultWithContext // Embed the interface
}

// Override methods used
func (m *MockResult) Single(ctx context.Context) (neo4j.Record, error) {
	args := m.Called(ctx)
	err := args.Error(1)
	if err != nil {
		var zeroRec neo4j.Record
		return zeroRec, err
	}
	rec := args.Get(0)
	if rec == nil {
		var zeroRec neo4j.Record
		return zeroRec, fmt.Errorf("mock error: Single() called but no return value set in mock")
	}
	return rec.(neo4j.Record), nil
}

func (m *MockResult) Collect(ctx context.Context) ([]neo4j.Record, error) {
	args := m.Called(ctx)
	err := args.Error(1)
	if err != nil {
		var zeroSlice []neo4j.Record
		return zeroSlice, err
	}
	rec := args.Get(0)
	if rec == nil {
		var zeroSlice []neo4j.Record
		return zeroSlice, fmt.Errorf("mock error: Collect() called but no return value set in mock")
	}
	return rec.([]neo4j.Record), nil
}

func (m *MockResult) Consume(ctx context.Context) (neo4j.ResultSummary, error) {
	args := m.Called(ctx)
	err := args.Error(1)
	if err != nil {
		var zeroSum neo4j.ResultSummary
		return zeroSum, err
	}
	sum := args.Get(0)
	if sum == nil {
		var zeroSum neo4j.ResultSummary
		return zeroSum, fmt.Errorf("mock error: Consume() called but no return value set in mock")
	}
	return sum.(neo4j.ResultSummary), nil
}

// MockRecord mocks neo4j.Record
type MockRecord struct {
	mock.Mock
	neo4j.Record // Embed the interface
	// Store mock data internally
	data map[string]any
}

// Override Get method
func (m *MockRecord) Get(key string) (any, bool) {
	// Delegate to mock setup OR use internal data
	args := m.Called(key)
	if len(args) > 0 { // Check if expectation was set
		return args.Get(0), args.Bool(1)
	}
	// Fallback to internal data if no specific expectation for Get was set
	val, ok := m.data[key]
	return val, ok

}

// Helper to set internal data for a mock record
func NewMockRecord(data map[string]any) *MockRecord {
	return &MockRecord{data: data}
}

// MockSummary mocks neo4j.ResultSummary
type MockSummary struct {
	mock.Mock
	neo4j.ResultSummary // Embed the interface
}

func (m *MockSummary) Counters() neo4j.Counters {
	args := m.Called()
	counters := args.Get(0)
	if counters == nil {
		// Return a default mock counters if none provided,
		// otherwise the test might panic if Counters() is called.
		return new(MockCounters)
	}
	return counters.(neo4j.Counters)
}

// MockCounters mocks neo4j.Counters
type MockCounters struct {
	mock.Mock
	neo4j.Counters // Embed the interface
}

func (m *MockCounters) NodesCreated() int {
	args := m.Called()
	return args.Int(0)
}

func (m *MockCounters) NodesDeleted() int {
	args := m.Called()
	return args.Int(0)
}

func (m *MockCounters) RelationshipsCreated() int {
	args := m.Called()
	return args.Int(0)
}

func (m *MockCounters) RelationshipsDeleted() int {
	args := m.Called()
	return args.Int(0)
}

// --- Test Cases ---

func TestNeo4jNodeDAL_ExecCreateNode(t *testing.T) {
	dal := NewNodeDAL()
	ctx := context.Background()

	nodeType := network.NodeType_PERSON
	properties := map[string]any{"id": "node1", "name": "Alice", "created_at": time.Now()}
	expectedQuery := fmt.Sprintf(`CREATE (n:%s $props) RETURN n`, nodeType.String())
	expectedParams := map[string]any{"props": properties}

	// Mock Node to be returned
	mockNode := dbtype.Node{Id: 1, Labels: []string{"PERSON"}, Props: properties}

	t.Run("Success", func(t *testing.T) {
		mockSession := new(MockSession)
		mockTx := new(MockTransaction)
		mockResult := new(MockResult)
		// mockRecord := new(MockRecord) - Using NewMockRecord helper
		mockRecord := NewMockRecord(map[string]any{"n": mockNode})

		// Setup expectations: Session -> Transaction -> Result -> Record
		mockSession.On("ExecuteWrite", ctx, mock.AnythingOfType("neo4j.ManagedTransactionWork"), mock.Anything).Return(mockNode, nil).Once()
		// The ExecuteWrite mock calls the 'work' func, which uses the Transaction mock
		// We need to retrieve the *specific* mockTx instance passed to the work function by the session mock
		// Note: This relies on the implementation detail of the MockSession above.
		mockSession.TestData().Set("mockTx", mockTx) // Prepare mockTx for work function

		mockTx.On("Run", ctx, expectedQuery, expectedParams).Return(mockResult, nil).Once()
		mockResult.On("Single", ctx).Return(mockRecord, nil).Once()
		// mockRecord.On("Get", "n").Return(mockNode, true).Once() // Not needed with NewMockRecord helper if data["n"] exists

		// Call the DAL function
		createdNode, err := dal.ExecCreateNode(ctx, mockSession, nodeType, properties)

		// Assertions
		assert.NoError(t, err)
		assert.Equal(t, mockNode, createdNode)
		mockSession.AssertExpectations(t)
		mockTx.AssertExpectations(t)
		mockResult.AssertExpectations(t)
		// mockRecord.AssertExpectations(t) // Not needed for NewMockRecord helper
	})

	t.Run("Error on Tx Run", func(t *testing.T) {
		mockSession := new(MockSession)
		mockTx := new(MockTransaction)
		expectedErr := errors.New("tx run failed")

		mockSession.On("ExecuteWrite", ctx, mock.AnythingOfType("neo4j.ManagedTransactionWork"), mock.Anything).Return(nil, nil).Once() // Expect work func to be called
		mockSession.TestData().Set("mockTx", mockTx)

		mockTx.On("Run", ctx, expectedQuery, expectedParams).Return(nil, expectedErr).Once()

		createdNode, err := dal.ExecCreateNode(ctx, mockSession, nodeType, properties)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), expectedErr.Error()) // Check if original error is wrapped
		assert.Equal(t, dbtype.Node{}, createdNode)          // Expect zero value node
		mockSession.AssertExpectations(t)
		mockTx.AssertExpectations(t)
	})

	t.Run("Error on Result Single", func(t *testing.T) {
		mockSession := new(MockSession)
		mockTx := new(MockTransaction)
		mockResult := new(MockResult)
		expectedErr := errors.New("result single failed")

		mockSession.On("ExecuteWrite", ctx, mock.AnythingOfType("neo4j.ManagedTransactionWork"), mock.Anything).Return(nil, nil).Once()
		mockSession.TestData().Set("mockTx", mockTx)

		mockTx.On("Run", ctx, expectedQuery, expectedParams).Return(mockResult, nil).Once()
		mockResult.On("Single", ctx).Return(nil, expectedErr).Once() // Mock Single to return error

		createdNode, err := dal.ExecCreateNode(ctx, mockSession, nodeType, properties)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), expectedErr.Error())
		assert.Equal(t, dbtype.Node{}, createdNode)
		mockSession.AssertExpectations(t)
		mockTx.AssertExpectations(t)
		mockResult.AssertExpectations(t)
	})

	t.Run("Error on Session ExecuteWrite", func(t *testing.T) {
		mockSession := new(MockSession)
		expectedErr := errors.New("session execute failed")

		// Mock ExecuteWrite to return error directly
		mockSession.On("ExecuteWrite", ctx, mock.AnythingOfType("neo4j.ManagedTransactionWork"), mock.Anything).Return(nil, expectedErr).Once()

		createdNode, err := dal.ExecCreateNode(ctx, mockSession, nodeType, properties)

		assert.Error(t, err)
		assert.Equal(t, expectedErr, err) // Should return the direct error
		assert.Equal(t, dbtype.Node{}, createdNode)
		mockSession.AssertExpectations(t)
	})

	t.Run("Error on Record Get (Type Assertion Fail)", func(t *testing.T) {
		mockSession := new(MockSession)
		mockTx := new(MockTransaction)
		mockResult := new(MockResult)
		mockRecord := NewMockRecord(map[string]any{"n": "not a node"}) // Invalid data type

		mockSession.On("ExecuteWrite", ctx, mock.AnythingOfType("neo4j.ManagedTransactionWork"), mock.Anything).Return(nil, nil).Once()
		mockSession.TestData().Set("mockTx", mockTx)

		mockTx.On("Run", ctx, expectedQuery, expectedParams).Return(mockResult, nil).Once()
		mockResult.On("Single", ctx).Return(mockRecord, nil).Once()

		// Call the DAL function
		createdNode, err := dal.ExecCreateNode(ctx, mockSession, nodeType, properties)

		// Assertions
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "DAL: 结果中的 'n' 不是有效的节点类型")
		assert.Equal(t, dbtype.Node{}, createdNode)
		mockSession.AssertExpectations(t)
		mockTx.AssertExpectations(t)
		mockResult.AssertExpectations(t)
	})
}

func TestNeo4jNodeDAL_ExecGetNodeByID(t *testing.T) {
	dal := NewNodeDAL()
	ctx := context.Background()
	testID := "node123"

	// 构造模拟节点和标签
	props := map[string]any{"id": testID, "name": "张三"}
	dbNode := dbtype.Node{Id: 100, Labels: []string{"PERSON"}, Props: props}
	labels := []string{"PERSON"}

	t.Run("成功返回节点和标签", func(t *testing.T) {
		mockSession := new(MockSession)
		// 模拟 ExecuteRead 直接返回结果 map
		mockSession.On(
			"ExecuteRead", ctx,
			mock.AnythingOfType("neo4j.ManagedTransactionWork"),
			mock.Anything,
		).Return(map[string]any{"node": dbNode, "labels": labels}, nil).Once()

		node, gotLabels, err := dal.ExecGetNodeByID(ctx, mockSession, testID)
		assert.NoError(t, err)
		assert.Equal(t, dbNode, node)
		assert.Equal(t, labels, gotLabels)
		mockSession.AssertExpectations(t)
	})

	t.Run("节点不存在时返回 nil", func(t *testing.T) {
		mockSession := new(MockSession)
		// ExecuteRead 返回 nil, nil 表示未找到
		mockSession.On(
			"ExecuteRead", ctx,
			mock.AnythingOfType("neo4j.ManagedTransactionWork"),
			mock.Anything,
		).Return(nil, nil).Once()

		node, gotLabels, err := dal.ExecGetNodeByID(ctx, mockSession, testID)
		assert.NoError(t, err)
		assert.Equal(t, dbtype.Node{}, node)
		assert.Nil(t, gotLabels)
		mockSession.AssertExpectations(t)
	})

	t.Run("ExecuteRead 出错时返回错误", func(t *testing.T) {
		mockSession := new(MockSession)
		expectedErr := errors.New("read transaction failed")
		mockSession.On(
			"ExecuteRead", ctx,
			mock.AnythingOfType("neo4j.ManagedTransactionWork"),
			mock.Anything,
		).Return(nil, expectedErr).Once()

		node, gotLabels, err := dal.ExecGetNodeByID(ctx, mockSession, testID)
		assert.Error(t, err)
		assert.Equal(t, expectedErr, err)
		assert.Equal(t, dbtype.Node{}, node)
		assert.Nil(t, gotLabels)
		mockSession.AssertExpectations(t)
	})
}

// TODO: Add test cases for other NodeDAL functions (ExecGetNodeByID, ExecUpdateNode, ExecDeleteNode, ExecSearchNodes, ExecGetNetwork, ExecGetPath)
