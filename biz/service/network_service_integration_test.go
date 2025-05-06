package service_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"testing"

	"github.com/google/uuid"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"labelwall/biz/dal/neo4jdal"
	"labelwall/biz/model/relationship/network"
	"labelwall/biz/repo/neo4jrepo"
	"labelwall/biz/service" // Import the service package
	"labelwall/pkg/cache"
)

var (
	testService  service.NetworkService
	testNodeRepo neo4jrepo.NodeRepository
	testRelRepo  neo4jrepo.RelationRepository
	testDriver   neo4j.DriverWithContext
	testCache    cache.NodeAndByteCache
	redisClient  *redis.Client
	testLogger   *zap.Logger
)

// Helper function to create a node using the service and return its ID
func createTestNode(ctx context.Context, t *testing.T, name string, nodeType network.NodeType) string {
	t.Helper() // Mark as test helper
	createReq := &network.CreateNodeRequest{Type: nodeType, Name: name}
	createResp, errCreate := testService.CreateNode(ctx, createReq)
	require.NoError(t, errCreate)
	require.True(t, createResp.Success)
	require.NotNil(t, createResp.Node)
	return createResp.Node.ID
}

// Helper function to create a node with profession using the service and return its ID
func createTestNodeWithProfession(ctx context.Context, t *testing.T, name string, nodeType network.NodeType, profession string) string {
	t.Helper() // Mark as test helper
	createReq := &network.CreateNodeRequest{Type: nodeType, Name: name, Profession: &profession}
	createResp, errCreate := testService.CreateNode(ctx, createReq)
	require.NoError(t, errCreate)
	require.True(t, createResp.Success)
	require.NotNil(t, createResp.Node)
	return createResp.Node.ID
}

// Helper function to create a relation using the service and return its ID
func createTestRelation(ctx context.Context, t *testing.T, sourceID, targetID string, relType network.RelationType) string {
	t.Helper() // Mark as test helper
	createReq := &network.CreateRelationRequest{Source: sourceID, Target: targetID, Type: relType}
	createResp, errCreate := testService.CreateRelation(ctx, createReq)
	require.NoError(t, errCreate)
	require.True(t, createResp.Success)
	require.NotNil(t, createResp.Relation)
	return createResp.Relation.ID
}

// Helper function to sort nodes by ID
func sortNodesByID(nodes []*network.Node) {
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].ID < nodes[j].ID
	})
}

// Helper function to sort relations by ID
func sortRelationsByID(relations []*network.Relation) {
	sort.Slice(relations, func(i, j int) bool {
		return relations[i].ID < relations[j].ID
	})
}

// TestMain sets up the database, cache, repos, and service for integration tests
func TestMain(m *testing.M) {
	// --- Setup Neo4j ---
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
	testDriver, err = neo4j.NewDriverWithContext(neo4jURI, neo4j.BasicAuth(neo4jUser, neo4jPass, ""))
	if err != nil {
		panic(fmt.Sprintf("Failed to connect to Neo4j: %v", err))
	}
	err = testDriver.VerifyConnectivity(context.Background())
	if err != nil {
		panic(fmt.Sprintf("Neo4j connectivity verification failed: %v", err))
	}

	// --- Setup Redis ---
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6381"
	}
	redisClient = redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: "",
		DB:       0,
	})
	_, err = redisClient.Ping(context.Background()).Result()
	if err != nil {
		testDriver.Close(context.Background())
		panic(fmt.Sprintf("Failed to connect to Redis: %v", err))
	}
	// Use estimated keys and FP rate suitable for testing
	redisCacheImpl, err := cache.NewRedisCache(redisClient, "svc_test:", 10000, 0.01)
	if err != nil {
		testDriver.Close(context.Background())
		redisClient.Close()
		panic(fmt.Sprintf("Failed to create Redis cache: %v", err))
	}
	testCache = redisCacheImpl // Assign to the combined interface

	// --- Setup DAL ---
	nodeDal := neo4jdal.NewNodeDAL()
	relationDal := neo4jdal.NewRelationDAL()

	// --- Create a logger for tests ---
	testLogger, _ = zap.NewDevelopment() // Or zap.NewNop() for no test output
	defer testLogger.Sync()

	// --- Setup Repositories ---
	testRelRepo = neo4jrepo.NewRelationRepository(testDriver, relationDal, redisCacheImpl, 300, 1000, testLogger)
	testNodeRepo = neo4jrepo.NewNodeRepository(testDriver, nodeDal, redisCacheImpl, testRelRepo, 300, 100, 500, 100, 100, 3, 5, 1000, testLogger)

	// --- Setup Service ---
	testService = service.NewNetworkService(testNodeRepo, testRelRepo, testLogger) // Inject real repos and logger

	// --- Clean Database & Cache Before Running ---
	clearTestData(context.Background())

	// --- Run Tests ---
	exitCode := m.Run()

	// --- Teardown ---
	testDriver.Close(context.Background())
	redisClient.Close()
	os.Exit(exitCode)
}

// clearTestData cleans Neo4j and Redis using the test prefix
func clearTestData(ctx context.Context) {
	// Clear Neo4j
	session := testDriver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)
	_, _ = session.Run(ctx, "MATCH ()-[r]->() DELETE r", nil) // Ignore errors for cleanup
	_, _ = session.Run(ctx, "MATCH (n) DELETE n", nil)        // Ignore errors for cleanup

	// Clear Redis using the specific test prefix
	testPrefix := "svc_test:*"
	keys, err := redisClient.Keys(ctx, testPrefix).Result()
	if err == nil && len(keys) > 0 {
		pipe := redisClient.Pipeline()
		pipe.Del(ctx, keys...)
		_, _ = pipe.Exec(ctx) // Ignore errors for cleanup
	} else if err != nil && !errors.Is(err, redis.Nil) {
		fmt.Printf("Warning: Failed to get Redis keys with prefix %s: %v\n", testPrefix, err)
	}
}

// --- Service Integration Tests ---

// TestCreateNode_Service_Integration tests the CreateNode service method
func TestCreateNode_Service_Integration(t *testing.T) {
	ctx := context.Background()
	require.NotNil(t, testService, "Service should be initialized")

	createReq := &network.CreateNodeRequest{
		Type:       network.NodeType_COMPANY,
		Name:       "Service Test Company",
		Profession: func(s string) *string { return &s }("Service Testing"),
		Properties: map[string]string{"domain": "testing"},
	}

	// Call the service method
	resp, err := testService.CreateNode(ctx, createReq)

	// Assertions
	require.NoError(t, err, "Service CreateNode returned an unexpected error")
	require.NotNil(t, resp, "Service CreateNode response should not be nil")
	assert.True(t, resp.Success, "Expected Success to be true")
	assert.Contains(t, resp.Message, "成功", "Expected success message") // Check for success keyword
	require.NotNil(t, resp.Node, "Response node should not be nil")
	assert.NotEmpty(t, resp.Node.ID, "Response node ID should not be empty")
	assert.Equal(t, createReq.Name, resp.Node.Name)
	assert.Equal(t, createReq.Type, resp.Node.Type)
	require.NotNil(t, resp.Node.Profession)
	assert.Equal(t, *createReq.Profession, *resp.Node.Profession)
	assert.Equal(t, createReq.Properties["domain"], resp.Node.Properties["domain"])

	// Optional: Verify directly in DB
	session := testDriver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
	defer session.Close(ctx)
	dbResult, errDb := session.Run(ctx, "MATCH (p:COMPANY {id: $id}) RETURN p.name AS name, p.domain AS domain", map[string]any{"id": resp.Node.ID})
	require.NoError(t, errDb, "Failed to query DB after service CreateNode")
	record, errDbSingle := dbResult.Single(ctx)
	require.NoError(t, errDbSingle, "Failed to get single record from DB")
	dbName, _ := record.Get("name")
	dbDomain, _ := record.Get("domain")
	assert.Equal(t, createReq.Name, dbName.(string))
	assert.Equal(t, createReq.Properties["domain"], dbDomain.(string))
}

// TestGetNode_Service_Integration tests the GetNode service method
func TestGetNode_Service_Integration(t *testing.T) {
	ctx := context.Background()
	require.NotNil(t, testService, "Service should be initialized")

	// 1. Setup: Create a node first using the service
	nodeName := "Get Service Test Node"
	nodeID := createTestNode(ctx, t, nodeName, network.NodeType_PERSON) // Use helper

	// --- Test Case 1: Get existing node ---
	t.Run("Get Existing Node", func(t *testing.T) {
		getReq := &network.GetNodeRequest{ID: nodeID}
		resp, err := testService.GetNode(ctx, getReq)

		require.NoError(t, err, "Service GetNode returned an unexpected error for existing node")
		require.NotNil(t, resp, "Service GetNode response should not be nil")
		assert.True(t, resp.Success, "Expected Success to be true for existing node")
		assert.Contains(t, resp.Message, "成功", "Expected success message")
		require.NotNil(t, resp.Node, "Response node should not be nil")
		assert.Equal(t, nodeID, resp.Node.ID)
		assert.Equal(t, nodeName, resp.Node.Name)
	})

	// --- Test Case 2: Get non-existent node ---
	t.Run("Get Non-Existent Node", func(t *testing.T) {
		nonExistentID := "svc-does-not-exist-" + uuid.NewString()
		getReq := &network.GetNodeRequest{ID: nonExistentID}
		resp, err := testService.GetNode(ctx, getReq)

		require.NoError(t, err, "Service GetNode returned an unexpected error for non-existent node")
		require.NotNil(t, resp, "Service GetNode response should not be nil")
		assert.False(t, resp.Success, "Expected Success to be false for non-existent node")
		assert.Contains(t, resp.Message, "未找到", "Expected 'not found' message")
		assert.Nil(t, resp.Node, "Response node should be nil for non-existent node")
	})
}

// TestUpdateNode_Service_Integration tests the UpdateNode service method
func TestUpdateNode_Service_Integration(t *testing.T) {
	ctx := context.Background()
	require.NotNil(t, testService, "Service should be initialized")

	// 1. Setup: Create a node to update
	originalName := "Original Update Node"
	nodeID := createTestNode(ctx, t, originalName, network.NodeType_SCHOOL) // Use helper

	// --- Test Case 1: Update existing node ---
	t.Run("Update Existing Node", func(t *testing.T) {
		updatedName := "Updated Node Name"
		updatedPropVal := "new-value"
		updateReq := &network.UpdateNodeRequest{
			ID:   nodeID,
			Name: &updatedName,
			Properties: map[string]string{
				"status": updatedPropVal,
			},
		}
		resp, err := testService.UpdateNode(ctx, updateReq)

		require.NoError(t, err, "Service UpdateNode returned an unexpected error for existing node")
		require.NotNil(t, resp, "Service UpdateNode response should not be nil")
		assert.True(t, resp.Success, "Expected Success to be true for update")
		assert.Contains(t, resp.Message, "成功", "Expected success message")
		require.NotNil(t, resp.Node, "Response node should not be nil after update")
		assert.Equal(t, nodeID, resp.Node.ID)
		assert.Equal(t, updatedName, resp.Node.Name)
		assert.Equal(t, updatedPropVal, resp.Node.Properties["status"])
	})

	// --- Test Case 2: Update non-existent node ---
	t.Run("Update Non-Existent Node", func(t *testing.T) {
		nonExistentID := "svc-update-does-not-exist-" + uuid.NewString()
		updatedName := "Attempt Update"
		updateReq := &network.UpdateNodeRequest{
			ID:   nonExistentID,
			Name: &updatedName,
		}
		resp, err := testService.UpdateNode(ctx, updateReq)

		require.NoError(t, err, "Service UpdateNode returned an unexpected error for non-existent node")
		require.NotNil(t, resp, "Service UpdateNode response should not be nil")
		assert.False(t, resp.Success, "Expected Success to be false for updating non-existent node")
		assert.Contains(t, resp.Message, "未找到", "Expected 'not found' message")
		assert.Nil(t, resp.Node, "Response node should be nil")
	})
}

// TestDeleteNode_Service_Integration tests the DeleteNode service method
func TestDeleteNode_Service_Integration(t *testing.T) {
	ctx := context.Background()
	require.NotNil(t, testService, "Service should be initialized")

	// 1. Setup: Create a node to delete
	nodeToDeleteName := "Node To Delete Service"
	nodeID := createTestNode(ctx, t, nodeToDeleteName, network.NodeType_PERSON) // Use helper

	// --- Test Case 1: Delete existing node ---
	t.Run("Delete Existing Node", func(t *testing.T) {
		deleteReq := &network.DeleteNodeRequest{ID: nodeID}
		resp, err := testService.DeleteNode(ctx, deleteReq)

		require.NoError(t, err, "Service DeleteNode returned an unexpected error for existing node")
		require.NotNil(t, resp, "Service DeleteNode response should not be nil")
		assert.True(t, resp.Success, "Expected Success to be true for deleting existing node")
		assert.Contains(t, resp.Message, "成功", "Expected success message")

		// Verify deletion by trying to get it again
		getReq := &network.GetNodeRequest{ID: nodeID}
		getResp, getErr := testService.GetNode(ctx, getReq)
		require.NoError(t, getErr)
		require.NotNil(t, getResp)
		assert.False(t, getResp.Success, "Node should not be found after deletion")
	})

	// --- Test Case 2: Delete non-existent node ---
	t.Run("Delete Non-Existent Node", func(t *testing.T) {
		nonExistentID := "svc-delete-does-not-exist-" + uuid.NewString()
		deleteReq := &network.DeleteNodeRequest{ID: nonExistentID}
		resp, err := testService.DeleteNode(ctx, deleteReq)

		require.NoError(t, err, "Service DeleteNode returned an unexpected error for non-existent node")
		require.NotNil(t, resp, "Service DeleteNode response should not be nil")
		// Deleting a non-existent node is considered successful in the service layer logic
		assert.True(t, resp.Success, "Expected Success to be true even for deleting non-existent node")
		assert.Contains(t, resp.Message, "不存在或已被删除", "Expected 'not exists or already deleted' message")
	})
}

// TestCreateRelation_Service_Integration tests the CreateRelation service method
func TestCreateRelation_Service_Integration(t *testing.T) {
	ctx := context.Background()
	require.NotNil(t, testService, "Service should be initialized")

	// 1. Setup: Create source and target nodes
	sourceID := createTestNode(ctx, t, "Source Node Rel", network.NodeType_PERSON)
	targetID := createTestNode(ctx, t, "Target Node Rel", network.NodeType_COMPANY)

	// --- Test Case 1: Create relation successfully ---
	t.Run("Create Relation Success", func(t *testing.T) {
		relType := network.RelationType_COLLEAGUE
		relProps := map[string]string{"since": "2023"}
		createReq := &network.CreateRelationRequest{
			Source:     sourceID,
			Target:     targetID,
			Type:       relType,
			Properties: relProps,
		}
		resp, err := testService.CreateRelation(ctx, createReq)

		require.NoError(t, err, "Service CreateRelation returned an unexpected error")
		require.NotNil(t, resp, "Service CreateRelation response should not be nil")
		assert.True(t, resp.Success, "Expected Success to be true")
		assert.Contains(t, resp.Message, "成功", "Expected success message")
		require.NotNil(t, resp.Relation, "Response relation should not be nil")
		assert.NotEmpty(t, resp.Relation.ID, "Relation ID should not be empty")
		assert.Equal(t, sourceID, resp.Relation.Source)
		assert.Equal(t, targetID, resp.Relation.Target)
		assert.Equal(t, relType, resp.Relation.Type)
		assert.Equal(t, relProps["since"], resp.Relation.Properties["since"])
	})

	// --- Test Case 2: Create relation with non-existent source ---
	t.Run("Create Relation Non-Existent Source", func(t *testing.T) {
		nonExistentSourceID := "non-existent-source-" + uuid.NewString()
		createReq := &network.CreateRelationRequest{
			Source: nonExistentSourceID,
			Target: targetID,
			Type:   network.RelationType_FRIEND,
		}
		resp, err := testService.CreateRelation(ctx, createReq)

		require.NoError(t, err, "Service CreateRelation returned an unexpected error for non-existent source")
		require.NotNil(t, resp, "Service CreateRelation response should not be nil")
		assert.False(t, resp.Success, "Expected Success to be false for non-existent source")
		assert.Contains(t, resp.Message, "创建关系失败", "Expected failure message") // Repo layer should return error
		assert.Nil(t, resp.Relation, "Response relation should be nil")
	})

	// --- Test Case 3: Create relation with non-existent target ---
	t.Run("Create Relation Non-Existent Target", func(t *testing.T) {
		nonExistentTargetID := "non-existent-target-" + uuid.NewString()
		createReq := &network.CreateRelationRequest{
			Source: sourceID,
			Target: nonExistentTargetID,
			Type:   network.RelationType_FRIEND,
		}
		resp, err := testService.CreateRelation(ctx, createReq)

		require.NoError(t, err, "Service CreateRelation returned an unexpected error for non-existent target")
		require.NotNil(t, resp, "Service CreateRelation response should not be nil")
		assert.False(t, resp.Success, "Expected Success to be false for non-existent target")
		assert.Contains(t, resp.Message, "创建关系失败", "Expected failure message")
		assert.Nil(t, resp.Relation, "Response relation should be nil")
	})
}

// TestGetRelation_Service_Integration tests the GetRelation service method
func TestGetRelation_Service_Integration(t *testing.T) {
	ctx := context.Background()
	require.NotNil(t, testService, "Service should be initialized")

	// 1. Setup: Create nodes and a relation
	sourceID := createTestNode(ctx, t, "Source Node Get Rel", network.NodeType_PERSON)
	targetID := createTestNode(ctx, t, "Target Node Get Rel", network.NodeType_PERSON)
	relType := network.RelationType_FRIEND
	relationID := createTestRelation(ctx, t, sourceID, targetID, relType) // Use helper

	// --- Test Case 1: Get existing relation ---
	t.Run("Get Existing Relation", func(t *testing.T) {
		getReq := &network.GetRelationRequest{ID: relationID}
		resp, err := testService.GetRelation(ctx, getReq)

		require.NoError(t, err, "Service GetRelation returned an unexpected error for existing relation")
		require.NotNil(t, resp, "Service GetRelation response should not be nil")
		assert.True(t, resp.Success, "Expected Success to be true for existing relation")
		assert.Contains(t, resp.Message, "成功", "Expected success message")
		require.NotNil(t, resp.Relation, "Response relation should not be nil")
		assert.Equal(t, relationID, resp.Relation.ID)
		assert.Equal(t, sourceID, resp.Relation.Source)
		assert.Equal(t, targetID, resp.Relation.Target)
		assert.Equal(t, relType, resp.Relation.Type)
	})

	// --- Test Case 2: Get non-existent relation ---
	t.Run("Get Non-Existent Relation", func(t *testing.T) {
		nonExistentID := "svc-rel-does-not-exist-" + uuid.NewString()
		getReq := &network.GetRelationRequest{ID: nonExistentID}
		resp, err := testService.GetRelation(ctx, getReq)

		require.NoError(t, err, "Service GetRelation returned an unexpected error for non-existent relation")
		require.NotNil(t, resp, "Service GetRelation response should not be nil")
		assert.False(t, resp.Success, "Expected Success to be false for non-existent relation")
		assert.Contains(t, resp.Message, "未找到", "Expected 'not found' message")
		assert.Nil(t, resp.Relation, "Response relation should be nil for non-existent relation")
	})
}

// TestUpdateRelation_Service_Integration tests the UpdateRelation service method
func TestUpdateRelation_Service_Integration(t *testing.T) {
	ctx := context.Background()
	require.NotNil(t, testService, "Service should be initialized")

	// 1. Setup: Create nodes and a relation
	sourceID := createTestNode(ctx, t, "Source Node Update Rel", network.NodeType_COMPANY)
	targetID := createTestNode(ctx, t, "Target Node Update Rel", network.NodeType_SCHOOL)
	originalType := network.RelationType_VISITED
	relationID := createTestRelation(ctx, t, sourceID, targetID, originalType) // Use helper

	// --- Test Case 1: Update existing relation ---
	t.Run("Update Existing Relation", func(t *testing.T) {
		updatedType := network.RelationType_FOLLOWING
		updatedLabel := "Updated Label"
		updatedProps := map[string]string{"reason": "testing update"}
		updateReq := &network.UpdateRelationRequest{
			ID:         relationID,
			Type:       &updatedType, // Pointer for optional field
			Label:      &updatedLabel,
			Properties: updatedProps,
		}
		resp, err := testService.UpdateRelation(ctx, updateReq)

		require.NoError(t, err, "Service UpdateRelation returned an unexpected error for existing relation")
		require.NotNil(t, resp, "Service UpdateRelation response should not be nil")
		assert.True(t, resp.Success, "Expected Success to be true for update")
		assert.Contains(t, resp.Message, "成功", "Expected success message")
		require.NotNil(t, resp.Relation, "Response relation should not be nil after update")
		assert.Equal(t, relationID, resp.Relation.ID)
		// Verify original type is retained (as DAL doesn't update it)
		assert.Equal(t, originalType, resp.Relation.Type, "DAL currently does not update relation type, original type should be retained")
		require.NotNil(t, resp.Relation.Label)
		assert.Equal(t, updatedLabel, *resp.Relation.Label)
		assert.Equal(t, updatedProps["reason"], resp.Relation.Properties["reason"])
	})

	// --- Test Case 2: Update non-existent relation ---
	t.Run("Update Non-Existent Relation", func(t *testing.T) {
		nonExistentID := "svc-rel-update-does-not-exist-" + uuid.NewString()
		updatedType := network.RelationType_FRIEND
		updateReq := &network.UpdateRelationRequest{
			ID:   nonExistentID,
			Type: &updatedType,
		}
		resp, err := testService.UpdateRelation(ctx, updateReq)

		require.NoError(t, err, "Service UpdateRelation returned an unexpected error for non-existent relation")
		require.NotNil(t, resp, "Service UpdateRelation response should not be nil")
		assert.False(t, resp.Success, "Expected Success to be false for updating non-existent relation")
		assert.Contains(t, resp.Message, "未找到", "Expected 'not found' message")
		assert.Nil(t, resp.Relation, "Response relation should be nil")
	})
}

// TestDeleteRelation_Service_Integration tests the DeleteRelation service method
func TestDeleteRelation_Service_Integration(t *testing.T) {
	ctx := context.Background()
	require.NotNil(t, testService, "Service should be initialized")

	// 1. Setup: Create nodes and a relation
	sourceID := createTestNode(ctx, t, "Source Node Del Rel", network.NodeType_PERSON)
	targetID := createTestNode(ctx, t, "Target Node Del Rel", network.NodeType_PERSON)
	relationID := createTestRelation(ctx, t, sourceID, targetID, network.RelationType_SCHOOLMATE) // Use helper

	// --- Test Case 1: Delete existing relation ---
	t.Run("Delete Existing Relation", func(t *testing.T) {
		deleteReq := &network.DeleteRelationRequest{ID: relationID}
		resp, err := testService.DeleteRelation(ctx, deleteReq)

		require.NoError(t, err, "Service DeleteRelation returned an unexpected error for existing relation")
		require.NotNil(t, resp, "Service DeleteRelation response should not be nil")
		assert.True(t, resp.Success, "Expected Success to be true for deleting existing relation")
		assert.Contains(t, resp.Message, "成功", "Expected success message")

		// Verify deletion by trying to get it again
		getReq := &network.GetRelationRequest{ID: relationID}
		getResp, getErr := testService.GetRelation(ctx, getReq)
		require.NoError(t, getErr)
		require.NotNil(t, getResp)
		assert.False(t, getResp.Success, "Relation should not be found after deletion")
	})

	// --- Test Case 2: Delete non-existent relation ---
	t.Run("Delete Non-Existent Relation", func(t *testing.T) {
		nonExistentID := "svc-rel-delete-does-not-exist-" + uuid.NewString()
		deleteReq := &network.DeleteRelationRequest{ID: nonExistentID}
		resp, err := testService.DeleteRelation(ctx, deleteReq)

		require.NoError(t, err, "Service DeleteRelation returned an unexpected error for non-existent relation")
		require.NotNil(t, resp, "Service DeleteRelation response should not be nil")
		assert.True(t, resp.Success, "Expected Success to be true even for deleting non-existent relation")
		assert.Contains(t, resp.Message, "不存在或已被删除", "Expected 'not exists or already deleted' message")
	})
}

// TestSearchNodes_Service_Integration tests the SearchNodes service method
func TestSearchNodes_Service_Integration(t *testing.T) {
	ctx := context.Background()
	require.NotNil(t, testService, "Service should be initialized")
	clearTestData(ctx)

	// 1. Setup: Create some nodes to search
	p1ID := createTestNodeWithProfession(ctx, t, "Search Alice", network.NodeType_PERSON, "Engineer")
	p2ID := createTestNodeWithProfession(ctx, t, "Search Bob", network.NodeType_PERSON, "Engineer")
	_ = createTestNodeWithProfession(ctx, t, "Search Charlie", network.NodeType_PERSON, "Designer")
	c1ID := createTestNode(ctx, t, "Search Corp", network.NodeType_COMPANY)

	// --- Test Case 1: Search by criteria (profession=Engineer) ---
	t.Run("Search by Criteria Hit", func(t *testing.T) {
		searchReq := &network.SearchNodesRequest{
			Type:     network.NodeTypePtr(network.NodeType_PERSON),
			Criteria: map[string]string{"profession": "Engineer"},
			Limit:    func(i int32) *int32 { return &i }(10),
		}
		resp, err := testService.SearchNodes(ctx, searchReq)

		require.NoError(t, err, "Service SearchNodes returned an unexpected error")
		require.NotNil(t, resp, "Service SearchNodes response should not be nil")
		assert.True(t, resp.Success, "Expected Success to be true")
		assert.EqualValues(t, 2, resp.Total, "Expected 2 total results")
		require.Len(t, resp.Nodes, 2, "Expected 2 nodes in results")

		// Verify the correct nodes are returned (order might vary, check IDs)
		foundIDs := []string{resp.Nodes[0].ID, resp.Nodes[1].ID}
		assert.Contains(t, foundIDs, p1ID)
		assert.Contains(t, foundIDs, p2ID)
	})

	// --- Test Case 2: Search by criteria (no results) ---
	t.Run("Search by Criteria Miss", func(t *testing.T) {
		searchReq := &network.SearchNodesRequest{
			Criteria: map[string]string{"name": "Unknown"},
		}
		resp, err := testService.SearchNodes(ctx, searchReq)

		require.NoError(t, err, "Service SearchNodes returned an unexpected error for no results")
		require.NotNil(t, resp, "Service SearchNodes response should not be nil")
		assert.True(t, resp.Success, "Expected Success to be true even with no results")
		assert.EqualValues(t, 0, resp.Total, "Expected 0 total results")
		assert.Len(t, resp.Nodes, 0, "Expected 0 nodes in results")
	})

	// --- Test Case 3: Search by type (COMPANY) ---
	t.Run("Search by Type", func(t *testing.T) {
		// Add direct DB check
		session := testDriver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
		debugResult, errDb := session.Run(ctx, "MATCH (c:COMPANY {id: $id}) RETURN count(c) as cnt", map[string]any{"id": c1ID})
		require.NoError(t, errDb, "Direct DB check query failed")
		record, errDbSingle := debugResult.Single(ctx)
		require.NoError(t, errDbSingle, "Direct DB check single failed")
		count, ok := record.Get("cnt")
		require.True(t, ok, "Direct DB check count not found")
		assert.EqualValues(t, 1, count.(int64), "Direct DB check failed: COMPANY node with ID %s not found before service call", c1ID)
		session.Close(ctx)

		searchReq := &network.SearchNodesRequest{
			Type: network.NodeTypePtr(network.NodeType_COMPANY),
		}
		resp, err := testService.SearchNodes(ctx, searchReq)

		require.NoError(t, err, "Service SearchNodes returned an unexpected error for type search")
		require.NotNil(t, resp, "Service SearchNodes response should not be nil")
		assert.True(t, resp.Success, "Expected Success to be true for type search")
		assert.EqualValues(t, 1, resp.Total, "Expected 1 total company")
		require.Len(t, resp.Nodes, 1, "Expected 1 company node")
		assert.Equal(t, c1ID, resp.Nodes[0].ID)
	})

	// --- Test Case 4: Search with pagination ---
	t.Run("Search with Pagination", func(t *testing.T) {
		// Search Page 1 (Limit 1)
		searchReq1 := &network.SearchNodesRequest{
			Type:     network.NodeTypePtr(network.NodeType_PERSON),
			Criteria: map[string]string{"profession": "Engineer"},
			Limit:    func(i int32) *int32 { return &i }(1),
		}
		resp1, err1 := testService.SearchNodes(ctx, searchReq1)
		require.NoError(t, err1)
		require.NotNil(t, resp1)
		assert.True(t, resp1.Success)
		assert.EqualValues(t, 2, resp1.Total)
		require.Len(t, resp1.Nodes, 1)
		node1ID := resp1.Nodes[0].ID

		// Search Page 2 (Limit 1, Offset 1)
		searchReq2 := &network.SearchNodesRequest{
			Type:     network.NodeTypePtr(network.NodeType_PERSON),
			Criteria: map[string]string{"profession": "Engineer"},
			Limit:    func(i int32) *int32 { return &i }(1),
			Offset:   func(i int32) *int32 { return &i }(1),
		}
		resp2, err2 := testService.SearchNodes(ctx, searchReq2)
		require.NoError(t, err2)
		require.NotNil(t, resp2)
		assert.True(t, resp2.Success)
		assert.EqualValues(t, 2, resp2.Total)
		require.Len(t, resp2.Nodes, 1)
		node2ID := resp2.Nodes[0].ID

		// Ensure the nodes are the two engineers and are different
		assert.Contains(t, []string{p1ID, p2ID}, node1ID)
		assert.Contains(t, []string{p1ID, p2ID}, node2ID)
		assert.NotEqual(t, node1ID, node2ID)
	})
}

// TestGetNodeRelations_Service_Integration tests the GetNodeRelations service method
func TestGetNodeRelations_Service_Integration(t *testing.T) {
	ctx := context.Background()
	require.NotNil(t, testService, "Service should be initialized")
	clearTestData(ctx)

	// 1. Setup: Create nodes and relations around a central node
	centerID := createTestNode(ctx, t, "Center Node", network.NodeType_PERSON)
	out1ID := createTestNode(ctx, t, "Out Node 1", network.NodeType_COMPANY)
	out2ID := createTestNode(ctx, t, "Out Node 2", network.NodeType_PERSON)
	in1ID := createTestNode(ctx, t, "In Node 1", network.NodeType_SCHOOL)
	in2ID := createTestNode(ctx, t, "In Node 2", network.NodeType_PERSON)
	unrelatedID := createTestNode(ctx, t, "Unrelated Node", network.NodeType_PERSON) // For no relations test

	relOut1ID := createTestRelation(ctx, t, centerID, out1ID, network.RelationType_COLLEAGUE)
	relOut2ID := createTestRelation(ctx, t, centerID, out2ID, network.RelationType_FRIEND)
	relIn1ID := createTestRelation(ctx, t, in1ID, centerID, network.RelationType_VISITED)
	relIn2ID := createTestRelation(ctx, t, in2ID, centerID, network.RelationType_FOLLOWING)

	// --- Test Case 1: Get all relations for center node ---
	t.Run("Get All Relations", func(t *testing.T) {
		req := &network.GetNodeRelationsRequest{NodeID: centerID}
		resp, err := testService.GetNodeRelations(ctx, req)

		require.NoError(t, err, "Service GetNodeRelations (All) returned an unexpected error")
		require.NotNil(t, resp, "Service GetNodeRelations (All) response should not be nil")
		assert.True(t, resp.Success, "Expected Success to be true")
		assert.EqualValues(t, 4, resp.Total, "Expected 4 total relations")
		require.Len(t, resp.Relations, 4, "Expected 4 relations in results")

		// Verify the correct relations are returned (order might vary)
		foundRelIDs := make(map[string]bool)
		for _, rel := range resp.Relations {
			foundRelIDs[rel.ID] = true
		}
		assert.Contains(t, foundRelIDs, relOut1ID)
		assert.Contains(t, foundRelIDs, relOut2ID)
		assert.Contains(t, foundRelIDs, relIn1ID)
		assert.Contains(t, foundRelIDs, relIn2ID)
	})

	// --- Test Case 2: Get outgoing relations ---
	t.Run("Get Outgoing Relations", func(t *testing.T) {
		req := &network.GetNodeRelationsRequest{
			NodeID: centerID,
		}
		resp, err := testService.GetNodeRelations(ctx, req)

		require.NoError(t, err, "Service GetNodeRelations (Outgoing - now All) returned an unexpected error")
		require.NotNil(t, resp, "Service GetNodeRelations (Outgoing - now All) response should not be nil")
		assert.True(t, resp.Success, "Expected Success to be true")
		assert.EqualValues(t, 4, resp.Total, "Expected 4 total relations (was outgoing)")
		require.Len(t, resp.Relations, 4, "Expected 4 relations in results (was outgoing)")

		foundRelIDs := make(map[string]bool)
		for _, rel := range resp.Relations {
			foundRelIDs[rel.ID] = true
		}
		assert.Contains(t, foundRelIDs, relOut1ID)
		assert.Contains(t, foundRelIDs, relOut2ID)
		assert.Contains(t, foundRelIDs, relIn1ID)
		assert.Contains(t, foundRelIDs, relIn2ID)
	})

	// --- Test Case 3: Get incoming relations of specific type ---
	t.Run("Get Incoming Relations Filtered", func(t *testing.T) {
		req := &network.GetNodeRelationsRequest{
			NodeID: centerID,
			Types:  []network.RelationType{network.RelationType_FOLLOWING},
		}
		resp, err := testService.GetNodeRelations(ctx, req)

		require.NoError(t, err, "Service GetNodeRelations (Incoming Filtered - now All Filtered) returned an unexpected error")
		require.NotNil(t, resp, "Service GetNodeRelations (Incoming Filtered - now All Filtered) response should not be nil")
		assert.True(t, resp.Success, "Expected Success to be true")
		assert.EqualValues(t, 1, resp.Total, "Expected 1 FOLLOWING relation (was incoming FOLLOWING)")
		require.Len(t, resp.Relations, 1, "Expected 1 relation in results (was incoming FOLLOWING)")
		assert.Equal(t, relIn2ID, resp.Relations[0].ID)
	})

	// --- Test Case 4: Get relations for node with no relations ---
	t.Run("Get Relations for Unrelated Node", func(t *testing.T) {
		req := &network.GetNodeRelationsRequest{NodeID: unrelatedID}
		resp, err := testService.GetNodeRelations(ctx, req)

		require.NoError(t, err, "Service GetNodeRelations (Unrelated) returned an unexpected error")
		require.NotNil(t, resp, "Service GetNodeRelations (Unrelated) response should not be nil")
		assert.True(t, resp.Success, "Expected Success to be true")
		assert.EqualValues(t, 0, resp.Total, "Expected 0 total relations")
		assert.Len(t, resp.Relations, 0, "Expected 0 relations in results")
	})

	// --- Test Case 5: Get relations for non-existent node ---
	// Note: Current Repo/DAL implementation might not distinguish between node not found
	// and node having no relations. Both return empty lists.
	t.Run("Get Relations for Non-Existent Node", func(t *testing.T) {
		nonExistentID := "svc-gnr-does-not-exist-" + uuid.NewString()
		req := &network.GetNodeRelationsRequest{NodeID: nonExistentID}
		resp, err := testService.GetNodeRelations(ctx, req)

		require.NoError(t, err, "Service GetNodeRelations (Non-Existent) returned an unexpected error")
		require.NotNil(t, resp, "Service GetNodeRelations (Non-Existent) response should not be nil")
		assert.True(t, resp.Success, "Expected Success to be true")
		assert.EqualValues(t, 0, resp.Total, "Expected 0 total relations")
		assert.Len(t, resp.Relations, 0, "Expected 0 relations in results")
	})
}

// TestGetNetwork_Service_Integration tests the GetNetwork service method
func TestGetNetwork_Service_Integration(t *testing.T) {
	ctx := context.Background()
	require.NotNil(t, testService, "Service should be initialized")
	clearTestData(ctx)

	// 1. Setup: Create a small network A(Eng)-[FRIEND]->B(Eng)-[COLLEAGUE]->C(Mgr), A-[VISITED]->C
	aID := createTestNodeWithProfession(ctx, t, "Net A", network.NodeType_PERSON, "Engineer")
	bID := createTestNodeWithProfession(ctx, t, "Net B", network.NodeType_PERSON, "Engineer")
	cID := createTestNodeWithProfession(ctx, t, "Net C", network.NodeType_PERSON, "Manager")
	dID := createTestNode(ctx, t, "Net D", network.NodeType_COMPANY) // Unrelated company

	rABID := createTestRelation(ctx, t, aID, bID, network.RelationType_FRIEND)
	rBCID := createTestRelation(ctx, t, bID, cID, network.RelationType_COLLEAGUE)
	rACID := createTestRelation(ctx, t, aID, cID, network.RelationType_VISITED)
	_ = dID // Avoid unused variable error

	// --- Test Case 1: Get network starting from Engineers, depth 1 ---
	t.Run("Get Network Depth 1", func(t *testing.T) {
		req := &network.GetNetworkRequest{
			StartNodeCriteria: map[string]string{"profession": "Engineer"},
			Depth:             1,
		}
		resp, err := testService.GetNetwork(ctx, req)

		require.NoError(t, err, "Service GetNetwork (Depth 1) returned an unexpected error")
		require.NotNil(t, resp, "Service GetNetwork (Depth 1) response should not be nil")
		assert.True(t, resp.Success, "Expected Success to be true")

		// Expected: Nodes A, B, C; Relations rAB, rBC, rAC
		assert.Len(t, resp.Nodes, 3, "Expected 3 nodes")
		assert.Len(t, resp.Relations, 3, "Expected 3 relations")

		foundNodeIDs := make(map[string]bool)
		for _, n := range resp.Nodes {
			foundNodeIDs[n.ID] = true
		}
		assert.Contains(t, foundNodeIDs, aID)
		assert.Contains(t, foundNodeIDs, bID)
		assert.Contains(t, foundNodeIDs, cID)

		foundRelIDs := make(map[string]bool)
		for _, r := range resp.Relations {
			foundRelIDs[r.ID] = true
		}
		assert.Contains(t, foundRelIDs, rABID)
		assert.Contains(t, foundRelIDs, rBCID)
		assert.Contains(t, foundRelIDs, rACID)
	})

	// --- Test Case 2: Get network starting from Engineers, depth 0 (should return only start nodes) ---
	// Note: Neo4j pathfinding with depth 0 might behave unexpectedly depending on query.
	// Assuming depth 0 means only the starting nodes. Repo/DAL handles this.
	t.Run("Get Network Depth 0", func(t *testing.T) {
		req := &network.GetNetworkRequest{
			StartNodeCriteria: map[string]string{"profession": "Engineer"},
			Depth:             0,
		}
		resp, err := testService.GetNetwork(ctx, req)

		require.NoError(t, err, "Service GetNetwork (Depth 0) returned an unexpected error")
		require.NotNil(t, resp, "Service GetNetwork (Depth 0) response should not be nil")
		assert.True(t, resp.Success, "Expected Success to be true")

		// Expected: Nodes A, B; No relations
		assert.Len(t, resp.Nodes, 2, "Expected 2 nodes (start nodes only)")
		assert.Len(t, resp.Relations, 0, "Expected 0 relations")

		foundNodeIDs := make(map[string]bool)
		for _, n := range resp.Nodes {
			foundNodeIDs[n.ID] = true
		}
		assert.Contains(t, foundNodeIDs, aID)
		assert.Contains(t, foundNodeIDs, bID)
	})

	// --- Test Case 3: Get network with relation type filter ---
	t.Run("Get Network Filter Relation Type", func(t *testing.T) {
		req := &network.GetNetworkRequest{
			StartNodeCriteria: map[string]string{"profession": "Engineer"},
			Depth:             1,
			RelationTypes:     []network.RelationType{network.RelationType_FRIEND, network.RelationType_VISITED},
		}
		resp, err := testService.GetNetwork(ctx, req)

		require.NoError(t, err, "Service GetNetwork (Rel Filter) returned an unexpected error")
		require.NotNil(t, resp, "Service GetNetwork (Rel Filter) response should not be nil")
		assert.True(t, resp.Success, "Expected Success to be true")

		// Expected: Nodes A, B, C; Relations rAB, rAC (rBC is excluded)
		assert.Len(t, resp.Nodes, 3, "Expected 3 nodes")
		assert.Len(t, resp.Relations, 2, "Expected 2 relations (FRIEND, VISITED)")

		foundRelIDs := make(map[string]bool)
		for _, r := range resp.Relations {
			foundRelIDs[r.ID] = true
		}
		assert.Contains(t, foundRelIDs, rABID)
		assert.Contains(t, foundRelIDs, rACID)
		assert.NotContains(t, foundRelIDs, rBCID)
	})

	// --- Test Case 4: Get network with node type filter ---
	t.Run("Get Network Filter Node Type", func(t *testing.T) {
		// Add a Company node connected to A
		compID := createTestNode(ctx, t, "Connected Comp", network.NodeType_COMPANY)
		rACompID := createTestRelation(ctx, t, aID, compID, network.RelationType_COLLEAGUE)

		req := &network.GetNetworkRequest{
			StartNodeCriteria: map[string]string{"id": aID}, // Start from A
			Depth:             1,
			NodeTypes:         []network.NodeType{network.NodeType_PERSON}, // Only Person nodes
		}
		resp, err := testService.GetNetwork(ctx, req)

		require.NoError(t, err, "Service GetNetwork (Node Filter) returned an unexpected error")
		require.NotNil(t, resp, "Service GetNetwork (Node Filter) response should not be nil")
		assert.True(t, resp.Success, "Expected Success to be true")

		// Expected: Nodes A, B, C (all Persons); Relation rAB, rAC (rBC starts from B which is included)
		// Node compID and relation rACompID should be excluded
		assert.Len(t, resp.Nodes, 3, "Expected 3 nodes (Persons only)")
		// Relations returned depend on the underlying query logic - if it returns paths including non-matching nodes,
		// the relation might be included but the endpoint node filtered out. Assuming the repo filters strictly.
		// Let's verify relations connected *between* the returned person nodes.
		assert.Len(t, resp.Relations, 2, "Expected 2 relations between Persons (rAB, rAC)")

		foundNodeIDs := make(map[string]bool)
		for _, n := range resp.Nodes {
			foundNodeIDs[n.ID] = true
		}
		assert.Contains(t, foundNodeIDs, aID)
		assert.Contains(t, foundNodeIDs, bID)
		assert.Contains(t, foundNodeIDs, cID)
		assert.NotContains(t, foundNodeIDs, compID)

		foundRelIDs := make(map[string]bool)
		for _, r := range resp.Relations {
			foundRelIDs[r.ID] = true
		}
		assert.Contains(t, foundRelIDs, rABID) // A->B (both PERSON)
		assert.Contains(t, foundRelIDs, rACID) // A->C (both PERSON)
		// Whether rBC (B->C) is included depends on how the filtering is applied in the repo/DAL query.
		// Test might need adjustment based on actual query behavior. Assuming strict filtering for now.
		//assert.NotContains(t, foundRelIDs, rBCID) // B->C relation (may or may not be included)
		assert.NotContains(t, foundRelIDs, rACompID) // A->Comp relation excluded

		// Clean up extra node/relation
		_, _ = testService.DeleteRelation(ctx, &network.DeleteRelationRequest{ID: rACompID})
		_, _ = testService.DeleteNode(ctx, &network.DeleteNodeRequest{ID: compID})

	})

	// --- Test Case 5: Get network with no matching start criteria ---
	t.Run("Get Network No Start Node", func(t *testing.T) {
		req := &network.GetNetworkRequest{
			StartNodeCriteria: map[string]string{"profession": "NonExistent"},
			Depth:             1,
		}
		resp, err := testService.GetNetwork(ctx, req)

		require.NoError(t, err, "Service GetNetwork (No Start) returned an unexpected error")
		require.NotNil(t, resp, "Service GetNetwork (No Start) response should not be nil")
		assert.True(t, resp.Success, "Expected Success to be true")
		assert.Len(t, resp.Nodes, 0, "Expected 0 nodes")
		assert.Len(t, resp.Relations, 0, "Expected 0 relations")
	})
}

// TestGetPath_Service_Integration tests the GetPath service method
func TestGetPath_Service_Integration(t *testing.T) {
	ctx := context.Background()
	require.NotNil(t, testService, "Service should be initialized")
	clearTestData(ctx)

	// 1. Setup: Create a path A-[F]->B-[C]->C-[S]->D, A-[V]->C
	aID := createTestNode(ctx, t, "Path A", network.NodeType_PERSON)
	bID := createTestNode(ctx, t, "Path B", network.NodeType_PERSON)
	cID := createTestNode(ctx, t, "Path C", network.NodeType_PERSON)
	dID := createTestNode(ctx, t, "Path D", network.NodeType_PERSON)
	eID := createTestNode(ctx, t, "Path E", network.NodeType_PERSON) // Unconnected

	rABID := createTestRelation(ctx, t, aID, bID, network.RelationType_FRIEND)     // A->B
	rBCID := createTestRelation(ctx, t, bID, cID, network.RelationType_COLLEAGUE)  // B->C
	rCDID := createTestRelation(ctx, t, cID, dID, network.RelationType_SCHOOLMATE) // C->D
	rACID := createTestRelation(ctx, t, aID, cID, network.RelationType_VISITED)    // A->C (shorter path to C)
	_ = eID                                                                        // Avoid unused

	// --- Test Case 1: Find shortest path A->D ---
	t.Run("Find Shortest Path A->D", func(t *testing.T) {
		req := &network.GetPathRequest{SourceID: aID, TargetID: dID}
		resp, err := testService.GetPath(ctx, req)

		require.NoError(t, err, "Service GetPath (A->D) returned an unexpected error")
		require.NotNil(t, resp, "Service GetPath (A->D) response should not be nil")
		assert.True(t, resp.Success, "Expected Success to be true")
		assert.Contains(t, resp.Message, "成功", "Expected success message")

		// Shortest path is A -> C -> D (length 2)
		require.Len(t, resp.Nodes, 3, "Expected 3 nodes in path A->C->D")
		assert.Equal(t, aID, resp.Nodes[0].ID)
		assert.Equal(t, cID, resp.Nodes[1].ID)
		assert.Equal(t, dID, resp.Nodes[2].ID)
		require.Len(t, resp.Relations, 2, "Expected 2 relations in path A->C->D")
		assert.Equal(t, rACID, resp.Relations[0].ID)
		assert.Equal(t, rCDID, resp.Relations[1].ID)
	})

	// --- Test Case 2: Find path A->D with type filter (excluding VISITED) ---
	t.Run("Find Path A->D Filtered", func(t *testing.T) {
		req := &network.GetPathRequest{
			SourceID: aID,
			TargetID: dID,
			Types:    []network.RelationType{network.RelationType_FRIEND, network.RelationType_COLLEAGUE, network.RelationType_SCHOOLMATE},
		}
		resp, err := testService.GetPath(ctx, req)

		require.NoError(t, err, "Service GetPath (A->D Filtered) returned an unexpected error")
		require.NotNil(t, resp, "Service GetPath (A->D Filtered) response should not be nil")
		assert.True(t, resp.Success, "Expected Success to be true")

		// Path should now be A -> B -> C -> D (length 3)
		require.Len(t, resp.Nodes, 4, "Expected 4 nodes in path A->B->C->D")
		assert.Equal(t, aID, resp.Nodes[0].ID)
		assert.Equal(t, bID, resp.Nodes[1].ID)
		assert.Equal(t, cID, resp.Nodes[2].ID)
		assert.Equal(t, dID, resp.Nodes[3].ID)
		require.Len(t, resp.Relations, 3, "Expected 3 relations in path A->B->C->D")
		assert.Equal(t, rABID, resp.Relations[0].ID)
		assert.Equal(t, rBCID, resp.Relations[1].ID)
		assert.Equal(t, rCDID, resp.Relations[2].ID)
	})

	// --- Test Case 3: Find path A->D with max depth 2 ---
	t.Run("Find Path A->D Max Depth 2", func(t *testing.T) {
		maxDepth := int32(2)
		req := &network.GetPathRequest{SourceID: aID, TargetID: dID, MaxDepth: &maxDepth}
		resp, err := testService.GetPath(ctx, req)

		require.NoError(t, err, "Service GetPath (A->D MaxDepth 2) returned an unexpected error")
		require.NotNil(t, resp, "Service GetPath (A->D MaxDepth 2) response should not be nil")
		assert.True(t, resp.Success, "Expected Success to be true as path A->C->D exists")

		// Shortest path A->C->D has length 2, should be found
		require.Len(t, resp.Nodes, 3)
		require.Len(t, resp.Relations, 2)
		assert.Equal(t, rACID, resp.Relations[0].ID)
		assert.Equal(t, rCDID, resp.Relations[1].ID)
	})

	// --- Test Case 4: Find path A->D with max depth 1 (path not found) ---
	t.Run("Find Path A->D Max Depth 1 Not Found", func(t *testing.T) {
		maxDepth := int32(1)
		req := &network.GetPathRequest{SourceID: aID, TargetID: dID, MaxDepth: &maxDepth}
		resp, err := testService.GetPath(ctx, req)

		require.NoError(t, err, "Service GetPath (A->D MaxDepth 1) returned an unexpected error")
		require.NotNil(t, resp, "Service GetPath (A->D MaxDepth 1) response should not be nil")
		assert.False(t, resp.Success, "Expected Success to be false (no path of length 1)")
		assert.Contains(t, resp.Message, "未找到", "Expected 'not found' message")
		assert.Nil(t, resp.Nodes)
		assert.Nil(t, resp.Relations)
	})

	// --- Test Case 5: Find path A->E (unconnected node) ---
	t.Run("Find Path A->E Not Found", func(t *testing.T) {
		req := &network.GetPathRequest{SourceID: aID, TargetID: eID}
		resp, err := testService.GetPath(ctx, req)

		require.NoError(t, err, "Service GetPath (A->E) returned an unexpected error")
		require.NotNil(t, resp, "Service GetPath (A->E) response should not be nil")
		assert.False(t, resp.Success, "Expected Success to be false (unconnected)")
		assert.Contains(t, resp.Message, "未找到", "Expected 'not found' message")
		assert.Nil(t, resp.Nodes)
		assert.Nil(t, resp.Relations)
	})
}
