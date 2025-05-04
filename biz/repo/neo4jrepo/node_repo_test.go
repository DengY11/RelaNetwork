package neo4jrepo_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/redis/go-redis/v9" // Or your chosen Redis client library
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"labelwall/biz/dal/neo4jdal" // Import the DAL implementation package
	"labelwall/biz/model/relationship/network"
	"labelwall/biz/repo/neo4jrepo"
	"labelwall/pkg/cache" // Assuming RedisCache implementation here
)

var (
	testRepo    neo4jrepo.NodeRepository
	testDriver  neo4j.DriverWithContext
	testCache   cache.NodeAndByteCache
	redisClient *redis.Client
)

// TestMain sets up the database and cache connection for all tests in the package
func TestMain(m *testing.M) {
	// --- Setup Neo4j ---
	// Read connection details from environment variables or config
	neo4jURI := os.Getenv("NEO4J_URI") // e.g., "neo4j://localhost:7687"
	neo4jUser := os.Getenv("NEO4J_USER")
	neo4jPass := os.Getenv("NEO4J_PASS")
	if neo4jURI == "" {
		neo4jURI = "neo4j://localhost:7687" // Default fallback
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
		panic("Failed to connect to Neo4j: " + err.Error())
	}
	// Optional: Verify connectivity
	err = testDriver.VerifyConnectivity(context.Background())
	if err != nil {
		panic("Neo4j connectivity verification failed: " + err.Error())
	}

	// --- Setup Redis ---
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6381" // Default fallback
	}
	redisClient = redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	_, err = redisClient.Ping(context.Background()).Result()
	if err != nil {
		testDriver.Close(context.Background()) // Close Neo4j driver if Redis fails
		panic("Failed to connect to Redis: " + err.Error())
	}
	// Instantiate your Redis cache implementation using the correct constructor
	testCacheImpl, err := cache.NewRedisCache(redisClient, "testprefix:") // Use correct constructor and add a test prefix
	if err != nil {
		testDriver.Close(context.Background())
		redisClient.Close()
		panic("Failed to create Redis cache: " + err.Error())
	}
	testCache = testCacheImpl // Assign to the interface variable

	// --- Setup DAL ---
	// Assuming NewNodeDAL constructor exists in the neo4jdal package and takes the driver
	nodeDal := neo4jdal.NewNodeDAL() // Call constructor without arguments

	// --- Setup Repository ---
	// We need a real RelationRepository implementation or a minimal mock/stub if its methods aren't directly tested here
	// For now, let's assume a minimal stub or nil if NodeRepository methods don't strictly require it.
	// If RelationRepository methods *are* needed (like in GetNetwork tests), you'll need a real one too.
	var dummyRelRepo neo4jrepo.RelationRepository // Replace with real or functional stub if needed
	// Pass the real NodeDAL implementation instead of nil
	testRepo = neo4jrepo.NewNodeRepository(testDriver, nodeDal, testCache, dummyRelRepo)

	// --- Clean Database & Cache Before Running ---
	clearTestData(context.Background())

	// --- Run Tests ---
	exitCode := m.Run()

	// --- Teardown ---
	// Optional: Clean up again after tests
	// clearTestData(context.Background())
	testDriver.Close(context.Background())
	redisClient.Close()

	os.Exit(exitCode)
}

// clearTestData cleans Neo4j and Redis
func clearTestData(ctx context.Context) {
	// Clear Neo4j
	session := testDriver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)
	_, err := session.Run(ctx, "MATCH (n) DETACH DELETE n", nil)
	if err != nil {
		// Log or handle error, maybe panic if cleanup is critical
		fmt.Printf("Warning: Failed to clear Neo4j data: %v\n", err)
	}

	// Clear Redis (be careful not to clear unrelated keys if Redis is shared)
	// Use the EXPORTED constants (assuming they will be exported in node_repo.go)
	prefixesToClear := []string{
		"testprefix:node:*", // Use the prefix defined in TestMain
		neo4jrepo.SearchNodesCachePrefix + "*",
		neo4jrepo.GetNetworkCachePrefix + "*",
		neo4jrepo.GetPathCachePrefix + "*",
	}

	for _, prefix := range prefixesToClear {
		keys, err := redisClient.Keys(ctx, prefix).Result()
		if err == nil && len(keys) > 0 {
			pipe := redisClient.Pipeline()
			pipe.Del(ctx, keys...)
			_, err := pipe.Exec(ctx)
			if err != nil && !errors.Is(err, redis.Nil) { // Ignore nil error if keys expired between KEYS and DEL
				fmt.Printf("Warning: Failed to clear Redis keys with prefix %s: %v\n", prefix, err)
			}
		} else if err != nil && !errors.Is(err, redis.Nil) {
			fmt.Printf("Warning: Failed to get Redis keys with prefix %s: %v\n", prefix, err)
		}
	}
}

// Helper to create a node directly in DB for setup purposes
func createNodeDirectly(ctx context.Context, node *network.Node) error {
	session := testDriver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)

	properties := map[string]any{
		"id":         node.ID,
		"name":       node.Name,
		"created_at": time.Now().UTC(),
		"updated_at": time.Now().UTC(),
	}
	if node.Avatar != nil {
		properties["avatar"] = *node.Avatar
	}
	if node.Profession != nil {
		properties["profession"] = *node.Profession
	}
	if node.Properties != nil {
		for k, v := range node.Properties {
			if _, exists := properties[k]; !exists {
				properties[k] = v
			}
		}
	}

	cypher := fmt.Sprintf("CREATE (n:%s $props) RETURN n", node.Type.String()) // Assuming NodeType.String() returns the label
	_, err := session.Run(ctx, cypher, map[string]any{"props": properties})
	if err != nil {
		return fmt.Errorf("failed to create node directly: %w", err)
	}
	return nil
}

// TestCreateAndGetNode (Integration Test Example)
func TestCreateAndGetNode_Integration(t *testing.T) {
	ctx := context.Background()
	require.NotNil(t, testRepo, "Repository should be initialized")
	require.NotNil(t, testCache, "Cache should be initialized")

	// Ensure clean state for this specific test
	clearTestData(ctx) // Optional: Per-test cleanup

	createReq := &network.CreateNodeRequest{
		Type:       network.NodeType_COMPANY,
		Name:       "Integration Test Inc.",
		Profession: func(s string) *string { return &s }("Testing"),
		Properties: map[string]string{"hq": "Test City"},
	}

	// 1. Create Node
	createdNode, err := testRepo.CreateNode(ctx, createReq)
	require.NoError(t, err, "CreateNode failed")
	require.NotNil(t, createdNode, "CreateNode returned nil node")
	require.NotEmpty(t, createdNode.ID, "Created node ID is empty")
	assert.Equal(t, createReq.Name, createdNode.Name)
	assert.Equal(t, createReq.Type, createdNode.Type)
	assert.Equal(t, *createReq.Profession, *createdNode.Profession)
	assert.Equal(t, createReq.Properties["hq"], createdNode.Properties["hq"])

	nodeID := createdNode.ID

	// 2. Get Node (Cache Miss)
	t.Logf("Attempting first GetNode for ID: %s (expect cache miss)", nodeID)
	retrievedNode1, err := testRepo.GetNode(ctx, nodeID)
	require.NoError(t, err, "First GetNode failed")
	require.NotNil(t, retrievedNode1, "First GetNode returned nil")
	assert.Equal(t, nodeID, retrievedNode1.ID)
	assert.Equal(t, createReq.Name, retrievedNode1.Name)

	// 3. Verify Cache (Optional but recommended)
	// Give cache a very short time to process potential background writes if any
	time.Sleep(50 * time.Millisecond)
	cachedVal, err := testCache.GetNode(ctx, nodeID) // Use the underlying cache directly
	assert.NoError(t, err, "Checking cache directly failed")
	assert.NotNil(t, cachedVal, "Node should be in cache after GetNode")
	assert.Equal(t, nodeID, cachedVal.ID, "Cached node ID mismatch")

	// 4. Get Node (Cache Hit)
	t.Logf("Attempting second GetNode for ID: %s (expect cache hit)", nodeID)
	retrievedNode2, err := testRepo.GetNode(ctx, nodeID)
	require.NoError(t, err, "Second GetNode failed")
	require.NotNil(t, retrievedNode2, "Second GetNode returned nil")
	assert.Equal(t, nodeID, retrievedNode2.ID)
	// Simple way to check if it likely came from cache: pointer equality (if GetNode returns cached pointer)
	// Or add logging within GetNode to confirm hit/miss
	// assert.Same(t, retrievedNode1, retrievedNode2, "Second GetNode should return the same cached instance") // This depends on cache implementation

	// 5. Verify in Neo4j Directly (Optional)
	session := testDriver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
	defer session.Close(ctx)
	result, err := session.Run(ctx, "MATCH (c:COMPANY {id: $id}) RETURN c.name AS name", map[string]any{"id": nodeID})
	require.NoError(t, err, "Direct Neo4j query failed")
	record, err := result.Single(ctx)
	require.NoError(t, err, "Failed to get single record from Neo4j")
	dbName, ok := record.Get("name")
	assert.True(t, ok, "Name property not found in DB")
	assert.Equal(t, createReq.Name, dbName.(string), "Name in DB doesn't match")

	// Teardown for this test case (optional if TestMain handles cleanup)
	// clearTestData(ctx)
}

// TestUpdateNode_Integration tests the UpdateNode method
func TestUpdateNode_Integration(t *testing.T) {
	ctx := context.Background()
	require.NotNil(t, testRepo, "Repository should be initialized")
	require.NotNil(t, testCache, "Cache should be initialized")
	clearTestData(ctx)

	// 1. Setup: Create an initial node
	initialNode := &network.Node{
		ID:         "update-test-node-1",
		Type:       network.NodeType_PERSON,
		Name:       "Original Name",
		Profession: func(s string) *string { return &s }("Original Profession"),
		Properties: map[string]string{"status": "active"},
	}
	err := createNodeDirectly(ctx, initialNode) // Use helper to bypass repo's CreateNode
	require.NoError(t, err, "Failed to create initial node for update test")

	// 2. Pre-populate Cache (by calling GetNode)
	_, err = testRepo.GetNode(ctx, initialNode.ID)
	require.NoError(t, err, "Failed to get node to populate cache before update")
	// Verify it's in cache
	time.Sleep(50 * time.Millisecond) // Allow potential async cache write
	cachedValPre, err := testCache.GetNode(ctx, initialNode.ID)
	require.NoError(t, err, "Failed to get node from cache before update")
	require.NotNil(t, cachedValPre, "Node should be in cache before update")
	require.Equal(t, "Original Name", cachedValPre.Name)

	// 3. Update the node
	updatedName := "Updated Name"
	updatedProfession := "Updated Profession"
	updatedStatus := "inactive"
	updateReq := &network.UpdateNodeRequest{
		ID:         initialNode.ID,
		Name:       &updatedName,
		Profession: &updatedProfession,
		Properties: map[string]string{"status": updatedStatus, "new_prop": "new_val"},
	}

	updatedNode, err := testRepo.UpdateNode(ctx, updateReq)
	require.NoError(t, err, "UpdateNode failed")
	require.NotNil(t, updatedNode, "UpdateNode returned nil")

	// 4. Verify updated node fields
	assert.Equal(t, initialNode.ID, updatedNode.ID)
	assert.Equal(t, updatedName, updatedNode.Name)
	require.NotNil(t, updatedNode.Profession)
	assert.Equal(t, updatedProfession, *updatedNode.Profession)
	require.NotNil(t, updatedNode.Properties)
	assert.Equal(t, updatedStatus, updatedNode.Properties["status"], "Existing property 'status' not updated")
	assert.Equal(t, "new_val", updatedNode.Properties["new_prop"], "New property 'new_prop' not added")
	// Verify timestamp was updated (hard to check exact time, check it's recent)
	// assert.WithinDuration(t, time.Now(), updatedNode.UpdatedAt, 5*time.Second) // Removed due to missing UpdatedAt field

	// 5. Verify Cache Invalidation
	time.Sleep(50 * time.Millisecond) // Allow potential async cache delete
	cachedValPost, err := testCache.GetNode(ctx, initialNode.ID)
	assert.ErrorIs(t, err, cache.ErrNotFound, "Cache should be invalidated after update (GetNode should return ErrNotFound)")
	assert.Nil(t, cachedValPost, "Cached value should be nil after invalidation")

	// 6. Get Node again (should fetch updated data from DB and re-populate cache)
	retrievedNodePostUpdate, err := testRepo.GetNode(ctx, initialNode.ID)
	require.NoError(t, err, "GetNode after update failed")
	require.NotNil(t, retrievedNodePostUpdate, "GetNode after update returned nil")
	assert.Equal(t, updatedName, retrievedNodePostUpdate.Name)
	require.NotNil(t, retrievedNodePostUpdate.Profession)
	assert.Equal(t, updatedProfession, *retrievedNodePostUpdate.Profession)

	// 7. Verify Cache Re-population
	time.Sleep(50 * time.Millisecond)
	cachedValRepopulated, err := testCache.GetNode(ctx, initialNode.ID)
	require.NoError(t, err, "Failed to get node from cache after re-population")
	require.NotNil(t, cachedValRepopulated, "Cache should be re-populated after GetNode post-update")
	assert.Equal(t, updatedName, cachedValRepopulated.Name)

	// 8. Test Update Non-existent Node
	nonExistentID := "does-not-exist"
	updateReqNonExistent := &network.UpdateNodeRequest{
		ID:   nonExistentID,
		Name: &updatedName, // Content doesn't matter much here
	}
	_, err = testRepo.UpdateNode(ctx, updateReqNonExistent)
	// Expect a 'not found' error, check the specific error type if DAL provides one
	require.Error(t, err, "Updating non-existent node should return an error")
	// TODO: Check for specific 'not found' error type if your DAL/Repo layer defines/propagates one.
	// For now, just check that an error occurred.
	// Example check (if using a helper like isNotFoundError):
	// assert.True(t, isNotFoundError(err), "Error should indicate 'not found'")
}

// TestDeleteNode_Integration tests the DeleteNode method
func TestDeleteNode_Integration(t *testing.T) {
	ctx := context.Background()
	require.NotNil(t, testRepo, "Repository should be initialized")
	require.NotNil(t, testCache, "Cache should be initialized")
	clearTestData(ctx)

	nodeToDeleteID := "delete-test-node-1"

	// --- Test Case 1: Delete existing node (after caching) ---
	t.Run("Delete Existing Node After Caching", func(t *testing.T) {
		clearTestData(ctx) // Clean state for sub-test

		// 1. Setup: Create a node and cache it
		nodeToDelete := &network.Node{
			ID:   nodeToDeleteID,
			Type: network.NodeType_SCHOOL,
			Name: "Node To Be Deleted",
		}
		err := createNodeDirectly(ctx, nodeToDelete)
		require.NoError(t, err, "Failed to create node for delete test")

		// Cache it by getting it
		_, err = testRepo.GetNode(ctx, nodeToDeleteID)
		require.NoError(t, err, "Failed to get node to populate cache before delete")
		time.Sleep(50 * time.Millisecond) // Allow cache write
		cachedValPre, err := testCache.GetNode(ctx, nodeToDeleteID)
		require.NoError(t, err, "Failed to get node from cache before delete")
		require.NotNil(t, cachedValPre, "Node should be in cache before delete")

		// 2. Execute DeleteNode
		err = testRepo.DeleteNode(ctx, nodeToDeleteID)
		require.NoError(t, err, "DeleteNode failed for existing node")

		// 3. Verify Deletion from DB
		_, err = testRepo.GetNode(ctx, nodeToDeleteID) // Should trigger DB check
		// Expect a 'not found' error from GetNode after deletion
		require.Error(t, err, "GetNode should return error after node deletion")
		// TODO: Check for specific 'not found' error type if available
		// Example check (if using a helper like isNotFoundError):
		// assert.True(t, isNotFoundError(err), "GetNode error should indicate 'not found'")

		// 4. Verify Deletion from Cache
		time.Sleep(50 * time.Millisecond) // Allow potential async cache delete
		cachedValPost, err := testCache.GetNode(ctx, nodeToDeleteID)
		assert.ErrorIs(t, err, cache.ErrNotFound, "Cache should be invalidated after delete (GetNode should return ErrNotFound)")
		assert.Nil(t, cachedValPost, "Cached value should be nil after delete")
	})

	// --- Test Case 2: Delete existing node (not in cache) ---
	t.Run("Delete Existing Node Not In Cache", func(t *testing.T) {
		clearTestData(ctx) // Clean state

		// 1. Setup: Create a node directly, DO NOT cache it via GetNode
		nodeToDelete := &network.Node{
			ID:   nodeToDeleteID,
			Type: network.NodeType_SCHOOL,
			Name: "Node To Be Deleted (No Cache)",
		}
		err := createNodeDirectly(ctx, nodeToDelete)
		require.NoError(t, err, "Failed to create node for delete test (no cache)")

		// Verify it's NOT in cache initially
		cachedValPre, err := testCache.GetNode(ctx, nodeToDeleteID)
		assert.ErrorIs(t, err, cache.ErrNotFound, "Node should not be in cache initially")
		assert.Nil(t, cachedValPre)

		// 2. Execute DeleteNode
		err = testRepo.DeleteNode(ctx, nodeToDeleteID)
		require.NoError(t, err, "DeleteNode failed for existing node (not cached)")

		// 3. Verify Deletion from DB
		_, err = testRepo.GetNode(ctx, nodeToDeleteID)
		require.Error(t, err, "GetNode should return error after node deletion (not cached)")
		// TODO: Check for specific 'not found' error type

		// 4. Verify Cache (still shouldn't be there)
		cachedValPost, err := testCache.GetNode(ctx, nodeToDeleteID)
		assert.ErrorIs(t, err, cache.ErrNotFound, "Node should still not be in cache after delete")
		assert.Nil(t, cachedValPost)
	})

	// --- Test Case 3: Delete Non-existent Node ---
	t.Run("Delete Non-Existent Node", func(t *testing.T) {
		clearTestData(ctx) // Clean state

		nonExistentID := "does-not-exist-for-delete"

		// 1. Verify node and cache are empty for this ID
		_, errDb := testRepo.GetNode(ctx, nonExistentID)
		require.Error(t, errDb, "Node should not exist in DB initially")
		_, errCache := testCache.GetNode(ctx, nonExistentID)
		require.ErrorIs(t, errCache, cache.ErrNotFound, "Node should not exist in cache initially")

		// 2. Execute DeleteNode
		err := testRepo.DeleteNode(ctx, nonExistentID)

		// 3. Verify Result
		// DeleteNode should return the underlying 'not found' error from the DB/DAL layer.
		require.Error(t, err, "Deleting non-existent node should return an error")
		// TODO: Check for specific 'not found' error type propagated from DAL/DB.
		// Example check (if using a helper like isNotFoundError):
		// assert.True(t, isNotFoundError(err), "Error should indicate 'not found'")

		// 4. Verify Cache (should still be empty, maybe nil value if DeleteNode tries to delete)
		// The current implementation attempts cache deletion even on DB NotFound.
		cachedValPost, errCachePost := testCache.GetNode(ctx, nonExistentID)
		assert.ErrorIs(t, errCachePost, cache.ErrNotFound, "Cache should remain empty for non-existent ID after delete attempt")
		assert.Nil(t, cachedValPost)
	})
}

// --- Add tests for SearchNodes, GetNetwork, GetPath ---
