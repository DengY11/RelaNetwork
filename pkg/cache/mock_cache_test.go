package cache

import (
	"context"
	"errors" // Added for generic errors
	"testing"
	"time"

	network "labelwall/biz/model/relationship/network"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// --- Mock NodeCache ---

type MockNodeCache struct {
	mock.Mock
}

func (m *MockNodeCache) GetNode(ctx context.Context, id string) (*network.Node, error) {
	args := m.Called(ctx, id)
	node, _ := args.Get(0).(*network.Node)
	return node, args.Error(1)
}

func (m *MockNodeCache) SetNode(ctx context.Context, id string, node *network.Node, ttl time.Duration) error {
	args := m.Called(ctx, id, node, ttl)
	return args.Error(0)
}

func (m *MockNodeCache) DeleteNode(ctx context.Context, id string) error {
	args := m.Called(ctx, id)
	return args.Error(0)
}

// --- Mock RelationCache ---

type MockRelationCache struct {
	mock.Mock
}

func (m *MockRelationCache) GetRelation(ctx context.Context, id string) (*network.Relation, error) {
	args := m.Called(ctx, id)
	rel, _ := args.Get(0).(*network.Relation)
	return rel, args.Error(1)
}

func (m *MockRelationCache) SetRelation(ctx context.Context, id string, relation *network.Relation, ttl time.Duration) error {
	args := m.Called(ctx, id, relation, ttl)
	return args.Error(0)
}

func (m *MockRelationCache) DeleteRelation(ctx context.Context, id string) error {
	args := m.Called(ctx, id)
	return args.Error(0)
}

// --- Mock Generic Cache[[]byte] ---
// We need a specific mock for Cache[[]byte] to be embedded
type MockByteCache struct {
	mock.Mock
}

func (m *MockByteCache) Get(ctx context.Context, key string) ([]byte, error) {
	args := m.Called(ctx, key)
	// Handle nil case explicitly for []byte which can be nil or empty
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	val, _ := args.Get(0).([]byte)
	return val, args.Error(1)
}

func (m *MockByteCache) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	args := m.Called(ctx, key, value, ttl)
	return args.Error(0)
}

func (m *MockByteCache) Delete(ctx context.Context, key string) error {
	args := m.Called(ctx, key)
	return args.Error(0)
}

// --- Mock NodeAndByteCache ---

type MockNodeAndByteCache struct {
	MockNodeCache // Embed mock NodeCache
	MockByteCache // Embed mock Cache[[]byte]
}

// --- Mock RelationAndByteCache ---

type MockRelationAndByteCache struct {
	MockRelationCache // Embed mock RelationCache
	MockByteCache     // Embed mock Cache[[]byte]
}

// ==================== Test Cases ====================

var errSimulated = errors.New("simulated cache error")

// --- Test Cases for MockNodeCache ---

func TestMockNodeCache_GetNode_Hit(t *testing.T) {
	mockCache := new(MockNodeCache)
	ctx := context.Background()
	nodeID := "node-123"
	expectedNode := &network.Node{ID: nodeID, Name: "Test Node"}
	ttl := 5 * time.Minute

	mockCache.On("SetNode", ctx, nodeID, expectedNode, ttl).Return(nil).Once()
	err := mockCache.SetNode(ctx, nodeID, expectedNode, ttl)
	assert.NoError(t, err)

	mockCache.On("GetNode", ctx, nodeID).Return(expectedNode, nil).Once()
	node, err := mockCache.GetNode(ctx, nodeID)

	assert.NoError(t, err)
	assert.Equal(t, expectedNode, node)
	mockCache.AssertExpectations(t)
}

func TestMockNodeCache_GetNode_Miss(t *testing.T) {
	mockCache := new(MockNodeCache)
	ctx := context.Background()
	nodeID := "node-456"

	mockCache.On("GetNode", ctx, nodeID).Return(nil, ErrNotFound).Once()
	node, err := mockCache.GetNode(ctx, nodeID)

	assert.ErrorIs(t, err, ErrNotFound)
	assert.Nil(t, node)
	mockCache.AssertExpectations(t)
}

func TestMockNodeCache_GetNode_NilValue(t *testing.T) {
	mockCache := new(MockNodeCache)
	ctx := context.Background()
	nodeID := "node-nil"
	ttl := 1 * time.Minute

	mockCache.On("SetNode", ctx, nodeID, (*network.Node)(nil), ttl).Return(nil).Once()
	err := mockCache.SetNode(ctx, nodeID, nil, ttl)
	assert.NoError(t, err)

	mockCache.On("GetNode", ctx, nodeID).Return(nil, ErrNilValue).Once()
	node, err := mockCache.GetNode(ctx, nodeID)

	assert.ErrorIs(t, err, ErrNilValue)
	assert.Nil(t, node)
	mockCache.AssertExpectations(t)
}

func TestMockNodeCache_SetNode_Error(t *testing.T) {
	mockCache := new(MockNodeCache)
	ctx := context.Background()
	nodeID := "node-err"
	nodeToSet := &network.Node{ID: nodeID}
	ttl := 1 * time.Minute

	mockCache.On("SetNode", ctx, nodeID, nodeToSet, ttl).Return(errSimulated).Once()
	err := mockCache.SetNode(ctx, nodeID, nodeToSet, ttl)

	assert.ErrorIs(t, err, errSimulated)
	mockCache.AssertExpectations(t)
}

func TestMockNodeCache_DeleteNode(t *testing.T) {
	mockCache := new(MockNodeCache)
	ctx := context.Background()
	nodeID := "node-789"

	mockCache.On("DeleteNode", ctx, nodeID).Return(nil).Once()
	err := mockCache.DeleteNode(ctx, nodeID)

	assert.NoError(t, err)
	mockCache.AssertExpectations(t)
}

func TestMockNodeCache_DeleteNode_Error(t *testing.T) {
	mockCache := new(MockNodeCache)
	ctx := context.Background()
	nodeID := "node-del-err"

	mockCache.On("DeleteNode", ctx, nodeID).Return(errSimulated).Once()
	err := mockCache.DeleteNode(ctx, nodeID)

	assert.ErrorIs(t, err, errSimulated)
	mockCache.AssertExpectations(t)
}

// --- Test Cases for MockRelationCache ---

func TestMockRelationCache_GetRelation_Hit(t *testing.T) {
	mockCache := new(MockRelationCache)
	ctx := context.Background()
	relID := "rel-123"
	label := "FRIEND"
	expectedRel := &network.Relation{ID: relID, Label: &label}
	ttl := 5 * time.Minute

	mockCache.On("SetRelation", ctx, relID, expectedRel, ttl).Return(nil).Once()
	err := mockCache.SetRelation(ctx, relID, expectedRel, ttl)
	assert.NoError(t, err)

	mockCache.On("GetRelation", ctx, relID).Return(expectedRel, nil).Once()
	rel, err := mockCache.GetRelation(ctx, relID)

	assert.NoError(t, err)
	assert.Equal(t, expectedRel, rel)
	mockCache.AssertExpectations(t)
}

func TestMockRelationCache_GetRelation_Miss(t *testing.T) {
	mockCache := new(MockRelationCache)
	ctx := context.Background()
	relID := "rel-456"

	mockCache.On("GetRelation", ctx, relID).Return(nil, ErrNotFound).Once()
	rel, err := mockCache.GetRelation(ctx, relID)

	assert.ErrorIs(t, err, ErrNotFound)
	assert.Nil(t, rel)
	mockCache.AssertExpectations(t)
}

func TestMockRelationCache_SetRelation_Error(t *testing.T) {
	mockCache := new(MockRelationCache)
	ctx := context.Background()
	relID := "rel-err"
	relToSet := &network.Relation{ID: relID}
	ttl := 1 * time.Minute

	mockCache.On("SetRelation", ctx, relID, relToSet, ttl).Return(errSimulated).Once()
	err := mockCache.SetRelation(ctx, relID, relToSet, ttl)

	assert.ErrorIs(t, err, errSimulated)
	mockCache.AssertExpectations(t)
}

func TestMockRelationCache_DeleteRelation(t *testing.T) {
	mockCache := new(MockRelationCache)
	ctx := context.Background()
	relID := "rel-789"

	mockCache.On("DeleteRelation", ctx, relID).Return(nil).Once()
	err := mockCache.DeleteRelation(ctx, relID)

	assert.NoError(t, err)
	mockCache.AssertExpectations(t)
}

func TestMockRelationCache_DeleteRelation_Error(t *testing.T) {
	mockCache := new(MockRelationCache)
	ctx := context.Background()
	relID := "rel-del-err"

	mockCache.On("DeleteRelation", ctx, relID).Return(errSimulated).Once()
	err := mockCache.DeleteRelation(ctx, relID)

	assert.ErrorIs(t, err, errSimulated)
	mockCache.AssertExpectations(t)
}

// --- Test Cases for MockByteCache ---

func TestMockByteCache_Get_Hit(t *testing.T) {
	mockCache := new(MockByteCache)
	ctx := context.Background()
	key := "byte-key-1"
	expectedValue := []byte("some data")
	ttl := 2 * time.Minute

	mockCache.On("Set", ctx, key, expectedValue, ttl).Return(nil).Once()
	err := mockCache.Set(ctx, key, expectedValue, ttl)
	assert.NoError(t, err)

	mockCache.On("Get", ctx, key).Return(expectedValue, nil).Once()
	value, err := mockCache.Get(ctx, key)

	assert.NoError(t, err)
	assert.Equal(t, expectedValue, value)
	mockCache.AssertExpectations(t)
}

func TestMockByteCache_Get_Miss(t *testing.T) {
	mockCache := new(MockByteCache)
	ctx := context.Background()
	key := "byte-key-miss"

	mockCache.On("Get", ctx, key).Return(nil, ErrNotFound).Once()
	value, err := mockCache.Get(ctx, key)

	assert.ErrorIs(t, err, ErrNotFound)
	assert.Nil(t, value)
	mockCache.AssertExpectations(t)
}

func TestMockByteCache_Get_NilValue(t *testing.T) {
	mockCache := new(MockByteCache)
	ctx := context.Background()
	key := "byte-key-nil"
	ttl := 1 * time.Minute

	// Explicitly check for nil []byte
	mockCache.On("Set", ctx, key, ([]byte)(nil), ttl).Return(nil).Once()
	err := mockCache.Set(ctx, key, nil, ttl)
	assert.NoError(t, err)

	mockCache.On("Get", ctx, key).Return(nil, ErrNilValue).Once()
	value, err := mockCache.Get(ctx, key)

	assert.ErrorIs(t, err, ErrNilValue)
	assert.Nil(t, value) // Should be nil, not empty slice
	mockCache.AssertExpectations(t)
}

func TestMockByteCache_Set_Error(t *testing.T) {
	mockCache := new(MockByteCache)
	ctx := context.Background()
	key := "byte-key-err"
	valueToSet := []byte("error data")
	ttl := 1 * time.Minute

	mockCache.On("Set", ctx, key, valueToSet, ttl).Return(errSimulated).Once()
	err := mockCache.Set(ctx, key, valueToSet, ttl)

	assert.ErrorIs(t, err, errSimulated)
	mockCache.AssertExpectations(t)
}

func TestMockByteCache_Delete(t *testing.T) {
	mockCache := new(MockByteCache)
	ctx := context.Background()
	key := "byte-key-del"

	mockCache.On("Delete", ctx, key).Return(nil).Once()
	err := mockCache.Delete(ctx, key)

	assert.NoError(t, err)
	mockCache.AssertExpectations(t)
}

func TestMockByteCache_Delete_Error(t *testing.T) {
	mockCache := new(MockByteCache)
	ctx := context.Background()
	key := "byte-key-del-err"

	mockCache.On("Delete", ctx, key).Return(errSimulated).Once()
	err := mockCache.Delete(ctx, key)

	assert.ErrorIs(t, err, errSimulated)
	mockCache.AssertExpectations(t)
}

// --- Test Cases for Combined Mocks (Usage Example) ---

func TestMockNodeAndByteCache_Usage(t *testing.T) {
	mockCache := new(MockNodeAndByteCache) // Note: uses the combined mock type
	ctx := context.Background()
	nodeID := "combined-node-1"
	byteKey := "combined-byte-1"
	expectedNode := &network.Node{ID: nodeID, Name: "Combined Node"}
	expectedBytes := []byte("combined bytes")
	// ttl := 3 * time.Minute // Removed unused variable

	// Set expectation on the embedded MockNodeCache
	mockCache.MockNodeCache.On("GetNode", ctx, nodeID).Return(expectedNode, nil).Once()
	// Set expectation on the embedded MockByteCache
	mockCache.MockByteCache.On("Get", ctx, byteKey).Return(expectedBytes, nil).Once()

	// Act - Call methods via the combined interface
	node, errNode := mockCache.GetNode(ctx, nodeID) // Calls MockNodeCache.GetNode
	bytes, errBytes := mockCache.Get(ctx, byteKey)  // Calls MockByteCache.Get

	// Assert
	assert.NoError(t, errNode)
	assert.Equal(t, expectedNode, node)
	assert.NoError(t, errBytes)
	assert.Equal(t, expectedBytes, bytes)

	// Assert expectations on both embedded mocks
	mockCache.MockNodeCache.AssertExpectations(t)
	mockCache.MockByteCache.AssertExpectations(t)
}

func TestMockRelationAndByteCache_Usage(t *testing.T) {
	mockCache := new(MockRelationAndByteCache) // Combined mock type
	ctx := context.Background()
	relID := "combined-rel-1"
	byteKey := "combined-byte-2"
	// expectedRel := &network.Relation{ID: relID, Label: "COLLEAGUE"} // Removed unused variable
	expectedBytes := []byte("more combined bytes")
	ttl := 4 * time.Minute

	// Set expectation on the embedded MockRelationCache
	mockCache.MockRelationCache.On("DeleteRelation", ctx, relID).Return(nil).Once()
	// Set expectation on the embedded MockByteCache
	mockCache.MockByteCache.On("Set", ctx, byteKey, expectedBytes, ttl).Return(nil).Once()

	// Act
	errRel := mockCache.DeleteRelation(ctx, relID)              // Calls MockRelationCache.DeleteRelation
	errBytes := mockCache.Set(ctx, byteKey, expectedBytes, ttl) // Calls MockByteCache.Set

	// Assert
	assert.NoError(t, errRel)
	assert.NoError(t, errBytes)

	// Assert expectations
	mockCache.MockRelationCache.AssertExpectations(t)
	mockCache.MockByteCache.AssertExpectations(t)
}
