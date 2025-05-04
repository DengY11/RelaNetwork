package neo4jdal

import (
	"context"
	"errors"
	"testing"

	network "labelwall/biz/model/relationship/network"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j/dbtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// 测试 ExecCreateRelation
func TestNeo4jRelationDAL_ExecCreateRelation(t *testing.T) {
	dal := NewRelationDAL()
	ctx := context.Background()
	srcID, dstID := "A", "B"
	relType := network.RelationType_FRIEND
	props := map[string]any{"since": 2020}
	// 模拟返回的关系对象
	dummyRel := dbtype.Relationship{Id: 123, StartId: 1, EndId: 2, Type: relType.String(), Props: props}

	mockSession := new(MockSession)
	// stub ExecuteWrite 返回 dummyRel
	mockSession.On("ExecuteWrite", ctx, mock.AnythingOfType("neo4j.ManagedTransactionWork"), mock.Anything).
		Return(dummyRel, nil).Once()

	gotRel, err := dal.ExecCreateRelation(ctx, mockSession, srcID, dstID, relType, props)
	assert.NoError(t, err)
	assert.Equal(t, dummyRel, gotRel)
	mockSession.AssertExpectations(t)

	t.Run("写事务失败", func(t *testing.T) {
		mockSession := new(MockSession)
		expErr := errors.New("exec failed")
		mockSession.On("ExecuteWrite", ctx, mock.AnythingOfType("neo4j.ManagedTransactionWork"), mock.Anything).
			Return(nil, expErr).Once()

		gotRel, err := dal.ExecCreateRelation(ctx, mockSession, srcID, dstID, relType, props)
		assert.Error(t, err)
		assert.Equal(t, expErr, err)
		assert.Equal(t, dbtype.Relationship{}, gotRel)
		mockSession.AssertExpectations(t)
	})
}

// 测试 ExecGetRelationByID
func TestNeo4jRelationDAL_ExecGetRelationByID(t *testing.T) {
	dal := NewRelationDAL()
	ctx := context.Background()
	id := "rel123"
	relType := "FRIEND"
	src, dst := "A", "B"
	dummyRel := dbtype.Relationship{Id: 200, StartId: 1, EndId: 2, Type: relType}

	t.Run("找到关系", func(t *testing.T) {
		mockSession := new(MockSession)
		// stub ExecuteRead 返回 map
		mockSession.On("ExecuteRead", ctx, mock.AnythingOfType("neo4j.ManagedTransactionWork"), mock.Anything).
			Return(map[string]any{"rel": dummyRel, "type": relType, "sourceId": src, "targetId": dst}, nil).Once()

		gotRel, gotType, gotSrc, gotDst, err := dal.ExecGetRelationByID(ctx, mockSession, id)
		assert.NoError(t, err)
		assert.Equal(t, dummyRel, gotRel)
		assert.Equal(t, relType, gotType)
		assert.Equal(t, src, gotSrc)
		assert.Equal(t, dst, gotDst)
		mockSession.AssertExpectations(t)
	})

	t.Run("未找到关系", func(t *testing.T) {
		mockSession := new(MockSession)
		// stub 返回 ErrNotFound
		mockSession.On("ExecuteRead", ctx, mock.Anything, mock.Anything).Return(nil, ErrNotFound).Once()

		gotRel, gotType, gotSrc, gotDst, err := dal.ExecGetRelationByID(ctx, mockSession, id)
		require.ErrorIs(t, err, ErrNotFound)
		assert.Equal(t, dbtype.Relationship{}, gotRel)
		assert.Equal(t, "", gotType)
		assert.Equal(t, "", gotSrc)
		assert.Equal(t, "", gotDst)
		mockSession.AssertExpectations(t)
	})

	t.Run("读事务失败", func(t *testing.T) {
		mockSession := new(MockSession)
		expErr := errors.New("read fail")
		mockSession.On("ExecuteRead", ctx, mock.Anything, mock.Anything).Return(nil, expErr).Once()

		gotRel, _, _, _, err := dal.ExecGetRelationByID(ctx, mockSession, id)
		assert.Error(t, err)
		assert.Equal(t, expErr, err)
		assert.Equal(t, dbtype.Relationship{}, gotRel)
		mockSession.AssertExpectations(t)
	})
}

// 测试 ExecUpdateRelation
func TestNeo4jRelationDAL_ExecUpdateRelation(t *testing.T) {
	dal := NewRelationDAL()
	ctx := context.Background()
	id := "rel1"
	updates := map[string]any{"since": 2021}
	// 模拟更新返回值
	dummyRel := dbtype.Relationship{Id: 300, StartId: 1, EndId: 2, Type: "FRIEND", Props: map[string]any{"since": 2021}}
	relType := "FRIEND"
	src, dst := "A", "B"

	mockSession := new(MockSession)
	mockSession.On("ExecuteWrite", ctx, mock.Anything, mock.Anything).
		Return(map[string]any{"rel": dummyRel, "type": relType, "sourceId": src, "targetId": dst}, nil).Once()

	gotRel, gotType, gotSrc, gotDst, err := dal.ExecUpdateRelation(ctx, mockSession, id, updates)
	assert.NoError(t, err)
	assert.Equal(t, dummyRel, gotRel)
	assert.Equal(t, relType, gotType)
	assert.Equal(t, src, gotSrc)
	assert.Equal(t, dst, gotDst)
	mockSession.AssertExpectations(t)
}

// 测试 ExecDeleteRelation
func TestNeo4jRelationDAL_ExecDeleteRelation(t *testing.T) {
	dal := NewRelationDAL()
	ctx := context.Background()
	id := "rel1"

	t.Run("写事务失败", func(t *testing.T) {
		mockSession := new(MockSession)
		expErr := errors.New("delete fail")
		mockSession.On("ExecuteWrite", ctx, mock.Anything, mock.Anything).Return(nil, expErr).Once()

		err := dal.ExecDeleteRelation(ctx, mockSession, id)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), expErr.Error())
		mockSession.AssertExpectations(t)
	})

	t.Run("非预期返回类型", func(t *testing.T) {
		mockSession := new(MockSession)
		// stub 返回非 ResultSummary
		mockSession.On("ExecuteWrite", ctx, mock.Anything, mock.Anything).Return("bad", nil).Once()

		err := dal.ExecDeleteRelation(ctx, mockSession, id)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "非预期的结果类型")
		mockSession.AssertExpectations(t)
	})
}

// 测试 ExecGetNodeRelations
func TestNeo4jRelationDAL_ExecGetNodeRelations(t *testing.T) {
	dal := NewRelationDAL()
	ctx := context.Background()
	nodeID := "node1"
	types := []string{"FRIEND"}
	outgoing, incoming := true, false
	limit, offset := int64(2), int64(0)

	// 模拟返回值
	dummyR := dbtype.Relationship{Id: 400, StartId: 1, EndId: 2, Type: "FRIEND"}
	rels := []dbtype.Relationship{dummyR}
	typesList := []string{"FRIEND"}
	srcList := []string{"node1"}
	dstList := []string{"node2"}
	total := int64(1)

	mockSession := new(MockSession)
	mockSession.On("ExecuteRead", ctx, mock.Anything, mock.Anything).
		Return(map[string]any{"rels": rels, "types": typesList, "sourceIds": srcList, "targetIds": dstList, "total": total}, nil).Once()

	gotRels, gotTypes, gotSrc, gotDst, gotTotal, err := dal.ExecGetNodeRelations(ctx, mockSession, nodeID, types, outgoing, incoming, limit, offset)
	assert.NoError(t, err)
	assert.Equal(t, rels, gotRels)
	assert.Equal(t, typesList, gotTypes)
	assert.Equal(t, srcList, gotSrc)
	assert.Equal(t, dstList, gotDst)
	assert.Equal(t, total, gotTotal)
	mockSession.AssertExpectations(t)
}
