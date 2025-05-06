package neo4jrepo_test

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/redis/go-redis/v9" // Or your chosen Redis client library
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap" // <<< 添加 zap 导入

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

// RelTest variables needed by relation_repo_test.go are declared in that file.
// They are initialized here in TestMain using the same resources.
/*
var (
	relTestRelRepo       neo4jrepo.RelationRepository
	relTestNodeRepo      neo4jrepo.NodeRepository
	relTestDriver        neo4j.DriverWithContext
	relTestCacheClient   *redis.Client
	relTestRelByteCache  cache.RelationAndByteCache
	relTestNodeByteCache cache.NodeAndByteCache
)
*/

// Helper function to get pointer to NodeType
func nodeTypePtr(t network.NodeType) *network.NodeType {
	return &t
}

// Helper function to get pointer to RelationType
func relationTypePtr(t network.RelationType) *network.RelationType {
	return &t
}

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
	// Provide estimatedKeys (e.g., 10000) and fpRate (e.g., 0.01) for the Bloom filter.
	testCacheImpl, err := cache.NewRedisCache(redisClient, "testprefix:", 10000, 0.01) // Use correct constructor and add a test prefix
	if err != nil {
		testDriver.Close(context.Background())
		redisClient.Close()
		panic("Failed to create Redis cache: " + err.Error())
	}
	testCache = testCacheImpl // Assign to the interface variable

	// Use the same cache implementation for both node and relation tests
	relationCacheImpl := testCacheImpl // Assuming RelationAndByteCache is compatible or the same impl

	// --- Setup DAL ---
	nodeDal := neo4jdal.NewNodeDAL()
	relationDal := neo4jdal.NewRelationDAL() // Instantiate RelationDAL

	// --- Setup Repositories ---
	// Create RelationRepo first as NodeRepo depends on it
	testLogger, _ := zap.NewDevelopment() // 或者 zap.NewNop() 如果不希望看到任何测试日志
	defer testLogger.Sync()               // S इंपॉर्टेंट: Sync flushes any buffered log entries

	relationRepoInstance := neo4jrepo.NewRelationRepository(testDriver, relationDal, relationCacheImpl, 300, 1000, testLogger) // Use the specific cache impl, add default params
	// Create NodeRepo, injecting the created RelationRepo
	nodeRepoInstance := neo4jrepo.NewNodeRepository(testDriver, nodeDal, testCache, relationRepoInstance, 300, 100, 500, 100, 100, 3, 5, 1000, testLogger) // Add default params and logger

	// --- Assign to Global Test Variables (for node_repo_test.go) ---
	testRepo = nodeRepoInstance

	// --- Assign to Global RelTest Variables (for relation_repo_test.go) ---
	// These variables are declared in relation_repo_test.go
	relTestRelRepo = relationRepoInstance
	relTestNodeRepo = nodeRepoInstance      // Use the same node repo instance
	relTestDriver = testDriver              // Use the same driver
	relTestCacheClient = redisClient        // Use the same redis client
	relTestRelByteCache = relationCacheImpl // Use the relation cache implementation
	relTestNodeByteCache = testCache        // Use the node cache implementation

	// --- Clean Database & Cache Before Running ---
	clearTestData(context.Background())

	// --- Run Tests ---
	exitCode := m.Run()

	// --- Teardown ---
	testDriver.Close(context.Background())
	redisClient.Close()
	os.Exit(exitCode)
}

// clearTestData cleans Neo4j and Redis
func clearTestData(ctx context.Context) {
	// Clear Neo4j
	session := testDriver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})

	// Clear relationships first, then nodes
	_, errRel := session.Run(ctx, "MATCH ()-[r]->() DELETE r", nil)
	if errRel != nil {
		fmt.Printf("Warning: Failed to clear Neo4j relationships: %v\n", errRel)
	}
	_, errNode := session.Run(ctx, "MATCH (n) DELETE n", nil)
	if errNode != nil {
		fmt.Printf("Warning: Failed to clear Neo4j nodes: %v\n", errNode)
	}
	session.Close(ctx)

	// Clear Redis using known prefixes
	clearRedisCache(ctx) // Call the new helper
}

// clearRedisCache helper function to clear specific test prefixes from Redis.
func clearRedisCache(ctx context.Context) {
	if redisClient == nil {
		fmt.Println("Warning: redisClient not initialized, cannot clear Redis cache.")
		return
	}
	// Use the EXPORTED constants from neo4jrepo package
	basePrefix := "testprefix:"
	prefixesToClear := []string{
		basePrefix + neo4jrepo.NodeCachePrefix + "*",          // Node details
		basePrefix + neo4jrepo.RelationCachePrefix + "*",      // Relation details
		basePrefix + neo4jrepo.SearchNodesCachePrefix + "*",   // Search results IDs
		basePrefix + neo4jrepo.GetNetworkCachePrefix + "*",    // Network graph IDs
		basePrefix + neo4jrepo.GetPathCachePrefix + "*",       // Path IDs
		basePrefix + neo4jrepo.NodeRelationsCachePrefix + "*", // NodeRelations IDs
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

// Helper function to create a node directly in DB for setup purposes
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

	// Ensure ID is set if not provided
	if node.ID == "" {
		node.ID = uuid.NewString()
	}
	properties["id"] = node.ID // Make sure ID from struct is used

	cypher := fmt.Sprintf("CREATE (n:%s $props) RETURN n", node.Type.String()) // Assuming NodeType.String() returns the label
	_, err := session.Run(ctx, cypher, map[string]any{"props": properties})
	if err != nil {
		return fmt.Errorf("failed to create node directly: %w", err)
	}
	return nil
}

// Helper function to create a relation directly in DB for setup purposes
func createRelationDirectly(ctx context.Context, sourceID, targetID string, rel *network.Relation) error {
	session := testDriver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)

	properties := map[string]any{
		"created_at": time.Now().UTC(),
		"updated_at": time.Now().UTC(),
	}
	if rel.ID != "" {
		properties["id"] = rel.ID
	} else {
		properties["id"] = uuid.NewString() // Generate ID if not provided
	}
	if rel.Properties != nil {
		for k, v := range rel.Properties {
			if _, exists := properties[k]; !exists {
				properties[k] = v
			}
		}
	}

	cypher := fmt.Sprintf(`
		MATCH (a {id: $sourceId}), (b {id: $targetId})
		CREATE (a)-[r:%s $props]->(b)
		RETURN r
	`, rel.Type.String()) // Assuming RelationType.String() returns the label

	_, err := session.Run(ctx, cypher, map[string]any{
		"sourceId": sourceID,
		"targetId": targetID,
		"props":    properties,
	})
	if err != nil {
		return fmt.Errorf("failed to create relation directly (%s)-[%s]->(%s): %w", sourceID, rel.Type.String(), targetID, err)
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
		// Expect ErrNilValue because GetNode caches nil after DB delete
		assert.ErrorIs(t, err, cache.ErrNilValue, "Cache should contain nil value after delete (GetNode caches nil)")
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

		// 4. Verify Cache (still shouldn't be there, or maybe nil)
		cachedValPost, err := testCache.GetNode(ctx, nodeToDeleteID)
		// Expect ErrNilValue because DeleteNode -> GetNode verification -> Caches nil
		// OR ErrNotFound if GetNode wasn't called after Delete in this specific path.
		// Let's check for ErrNilValue as GetNode IS called in step 3.
		assert.ErrorIs(t, err, cache.ErrNilValue, "Cache should have nil value after delete and GetNode check")
		assert.Nil(t, cachedValPost)
	})

	// --- Test Case 3: Delete Non-existent Node ---
	t.Run("Delete Non-Existent Node", func(t *testing.T) {
		clearTestData(ctx) // Clean state

		nonExistentID := "does-not-exist-for-delete"

		// 1. Verify node does not exist in DB and check initial cache state
		_, errDb := testRepo.GetNode(ctx, nonExistentID)
		require.Error(t, errDb, "Node should not exist in DB initially")
		// Call GetNode first, which will cache nil if not found in DB
		_, errCache := testCache.GetNode(ctx, nonExistentID)
		// Expect ErrNilValue because the previous GetNode call cached the nil marker.
		assert.ErrorIs(t, errCache, cache.ErrNilValue, "Cache should have nil value after GetNode called for non-existent ID")

		// 2. Execute DeleteNode
		err := testRepo.DeleteNode(ctx, nonExistentID)

		// 3. Verify Result
		// DeleteNode should return the underlying 'not found' error from the DB/DAL layer.
		require.Error(t, err, "Deleting non-existent node should return an error")
		// TODO: Check for specific 'not found' error type propagated from DAL/DB.
		// Example check (if using a helper like isNotFoundError):
		// assert.True(t, isNotFoundError(err), "Error should indicate 'not found'")

		// 4. Verify Cache (after DeleteNode attempted on non-existent)
		// DeleteNode attempts cache invalidation even if DB node not found.
		// If the nil marker from the initial GetNode was present, DeleteNode removed it.
		cachedValPost, errCachePost := testCache.GetNode(ctx, nonExistentID)
		// Expect ErrNotFound because the nil marker should have been deleted.
		assert.ErrorIs(t, errCachePost, cache.ErrNotFound, "Cache should be empty (ErrNotFound) after DeleteNode attempted on non-existent ID")
		assert.Nil(t, cachedValPost)
	})
}

// --- Integration Test for SearchNodes ---

// Helper function to sort nodes by ID for consistent comparison
func sortNodesByID(nodes []*network.Node) {
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].ID < nodes[j].ID
	})
}

// Helper function to generate cache key for SearchNodes (mirroring repo logic)
func generateSearchNodesCacheKeyForTest(req *network.SearchNodesRequest) string {
	// Mirror the logic from neo4jNodeRepo.generateSearchNodesCacheKey
	// Note: This makes the test dependent on the internal key generation logic.
	// Alternatively, find the key by pattern matching in Redis if exact generation is complex/unstable.

	// 1. Sort criteria keys
	keys := make([]string, 0, len(req.Criteria))
	for k := range req.Criteria {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// 2. Build canonical criteria string
	var criteriaBuilder strings.Builder // Import strings
	for i, k := range keys {
		if i > 0 {
			criteriaBuilder.WriteString("|")
		}
		criteriaBuilder.WriteString(k)
		criteriaBuilder.WriteString("=")
		criteriaBuilder.WriteString(req.Criteria[k])
	}
	criteriaStr := criteriaBuilder.String()

	// 3. Hash criteria string
	hasher := sha1.New() // Import crypto/sha1
	hasher.Write([]byte(criteriaStr))
	criteriaHash := hex.EncodeToString(hasher.Sum(nil)) // Import encoding/hex

	// 4. Limit and Offset
	var limitVal, offsetVal int32
	if req.Limit != nil {
		limitVal = *req.Limit
	}
	if req.Offset != nil {
		offsetVal = *req.Offset
	}

	// 5. Node Type
	var nodeTypeStr string
	if req.Type != nil {
		nodeTypeStr = req.Type.String()
	} else {
		nodeTypeStr = "ANY" // Match repo logic
	}

	// 6. Format key
	return fmt.Sprintf("%s%s:%s:%d:%d", neo4jrepo.SearchNodesCachePrefix, criteriaHash, nodeTypeStr, limitVal, offsetVal)
}

// Define a local struct matching the unexported one for unmarshalling cache data
// Alternatively, export the struct from the neo4jrepo package
type searchNodesCacheValueForTest struct {
	NodeIDs []string `json:"node_ids"`
	Total   int32    `json:"total"`
}

func TestSearchNodes_Integration(t *testing.T) {
	ctx := context.Background()
	require.NotNil(t, testRepo, "Repository should be initialized")
	require.NotNil(t, testCache, "Cache should be initialized")
	clearTestData(ctx)

	// 1. Setup: Create several nodes
	nodesToCreate := []*network.Node{
		{ID: "search-p1", Type: network.NodeType_PERSON, Name: "Alice Smith", Profession: func(s string) *string { return &s }("Engineer")},
		{ID: "search-p2", Type: network.NodeType_PERSON, Name: "Bob Johnson", Profession: func(s string) *string { return &s }("Engineer")},
		{ID: "search-p3", Type: network.NodeType_PERSON, Name: "Charlie Brown", Profession: func(s string) *string { return &s }("Designer")},
		{ID: "search-c1", Type: network.NodeType_COMPANY, Name: "Alpha Corp"},
		{ID: "search-c2", Type: network.NodeType_COMPANY, Name: "Beta Inc."},
	}
	for _, node := range nodesToCreate {
		err := createNodeDirectly(ctx, node)
		require.NoError(t, err, "Failed to create node for search test: %s", node.ID)
	}
	// Pre-cache Alice Smith for later tests
	var err error
	_, err = testRepo.GetNode(ctx, "search-p1")
	require.NoError(t, err)

	// --- Test Case 1: Search by keyword (PERSON, "Alice Smith") - Cache Miss ---
	t.Run("Search Keyword Cache Miss", func(t *testing.T) {
		searchReq := &network.SearchNodesRequest{
			Type: nodeTypePtr(network.NodeType_PERSON), // Pass pointer
			// Use Criteria map instead of Keyword
			Criteria: map[string]string{"name": "Alice Smith"},
			Limit:    func(i int32) *int32 { return &i }(10),
			Offset:   func(i int32) *int32 { return &i }(0),
		}
		cacheKey := generateSearchNodesCacheKeyForTest(searchReq) // Generate expected cache key

		// Verify cache is initially empty for this search
		_, err := testCache.Get(ctx, cacheKey)
		assert.ErrorIs(t, err, cache.ErrNotFound, "Cache should be empty before first search")

		// Execute search
		nodes, total, err := testRepo.SearchNodes(ctx, searchReq)
		require.NoError(t, err, "SearchNodes failed")
		assert.EqualValues(t, 1, total, "Expected 1 total result")
		require.Len(t, nodes, 1, "Expected 1 node in results")
		assert.Equal(t, "search-p1", nodes[0].ID)
		assert.Equal(t, "Alice Smith", nodes[0].Name)

		// Verify cache population
		time.Sleep(50 * time.Millisecond) // Allow cache write
		cachedData, err := testCache.Get(ctx, cacheKey)
		require.NoError(t, err, "Failed to get data from cache after search")
		require.NotEmpty(t, cachedData, "Cache should contain data after search")

		// Verify cache content (structure and values)
		var cachedValue searchNodesCacheValueForTest // Use the local struct for test
		err = json.Unmarshal(cachedData, &cachedValue)
		require.NoError(t, err, "Failed to unmarshal cached search data")
		assert.EqualValues(t, 1, cachedValue.Total)
		require.Len(t, cachedValue.NodeIDs, 1)
		assert.Equal(t, "search-p1", cachedValue.NodeIDs[0])
	})

	// --- Test Case 2: Search by keyword (PERSON, "Alice Smith") - Cache Hit ---
	t.Run("Search Keyword Cache Hit", func(t *testing.T) {
		searchReq := &network.SearchNodesRequest{
			Type: nodeTypePtr(network.NodeType_PERSON), // Pass pointer
			// Use Criteria map
			Criteria: map[string]string{"name": "Alice Smith"},
			Limit:    func(i int32) *int32 { return &i }(10),
			Offset:   func(i int32) *int32 { return &i }(0),
		}
		cacheKey := generateSearchNodesCacheKeyForTest(searchReq)

		// Ensure cache key exists from previous test (or re-run miss test logic if needed)
		_, err := testCache.Get(ctx, cacheKey)
		require.NoError(t, err, "Cache key should exist for cache hit test")

		// Execute search again
		// Add logging in SearchNodes repo method to confirm cache hit if needed
		t.Log("Expecting SearchNodes cache hit...")
		nodes, total, err := testRepo.SearchNodes(ctx, searchReq)
		require.NoError(t, err, "SearchNodes (cache hit) failed")
		assert.EqualValues(t, 1, total, "Expected 1 total result (cache hit)")
		require.Len(t, nodes, 1, "Expected 1 node in results (cache hit)")
		assert.Equal(t, "search-p1", nodes[0].ID) // Alice Smith was pre-cached by GetNode earlier
		assert.Equal(t, "Alice Smith", nodes[0].Name)
		assert.Equal(t, "Engineer", *nodes[0].Profession) // Verify full node details are retrieved via GetNode
	})

	// --- Test Case 3: Search with Pagination (PERSON, "Engineer") ---
	t.Run("Search with Pagination", func(t *testing.T) {
		// Search Page 1 (Limit 1, Offset 0)
		searchReq1 := &network.SearchNodesRequest{
			Type: nodeTypePtr(network.NodeType_PERSON), // Pass pointer
			// Use Criteria map for profession
			Criteria: map[string]string{"profession": "Engineer"},
			Limit:    func(i int32) *int32 { return &i }(1),
			Offset:   func(i int32) *int32 { return &i }(0),
		}
		nodes1, total1, err1 := testRepo.SearchNodes(ctx, searchReq1)
		require.NoError(t, err1)
		assert.EqualValues(t, 2, total1, "Expected 2 total Engineers")
		require.Len(t, nodes1, 1, "Expected 1 node on page 1")
		// Note: Order depends on DB/DAL implementation (added ORDER BY n.name in DAL)
		firstNodeID := nodes1[0].ID
		assert.Contains(t, []string{"search-p1", "search-p2"}, firstNodeID) // Should be Alice or Bob

		// Search Page 2 (Limit 1, Offset 1)
		searchReq2 := &network.SearchNodesRequest{
			Type: nodeTypePtr(network.NodeType_PERSON), // Pass pointer
			// Use Criteria map for profession
			Criteria: map[string]string{"profession": "Engineer"},
			Limit:    func(i int32) *int32 { return &i }(1),
			Offset:   func(i int32) *int32 { return &i }(1),
		}
		nodes2, total2, err2 := testRepo.SearchNodes(ctx, searchReq2)
		require.NoError(t, err2)
		assert.EqualValues(t, 2, total2, "Expected 2 total Engineers (page 2)")
		require.Len(t, nodes2, 1, "Expected 1 node on page 2")
		secondNodeID := nodes2[0].ID
		assert.Contains(t, []string{"search-p1", "search-p2"}, secondNodeID) // Should be the other engineer
		assert.NotEqual(t, firstNodeID, secondNodeID, "Page 1 and Page 2 node should be different")

		// Verify caching for both pages
		time.Sleep(50 * time.Millisecond)
		cacheKey1 := generateSearchNodesCacheKeyForTest(searchReq1)
		cachedData1, errCache1 := testCache.Get(ctx, cacheKey1)
		require.NoError(t, errCache1)
		require.NotEmpty(t, cachedData1)
		var cachedVal1 searchNodesCacheValueForTest // Use local struct
		require.NoError(t, json.Unmarshal(cachedData1, &cachedVal1))
		assert.EqualValues(t, 2, cachedVal1.Total)
		require.Len(t, cachedVal1.NodeIDs, 1)
		assert.Equal(t, firstNodeID, cachedVal1.NodeIDs[0])

		cacheKey2 := generateSearchNodesCacheKeyForTest(searchReq2)
		cachedData2, errCache2 := testCache.Get(ctx, cacheKey2)
		require.NoError(t, errCache2)
		require.NotEmpty(t, cachedData2)
		var cachedVal2 searchNodesCacheValueForTest // Use local struct
		require.NoError(t, json.Unmarshal(cachedData2, &cachedVal2))
		assert.EqualValues(t, 2, cachedVal2.Total)
		require.Len(t, cachedVal2.NodeIDs, 1)
		assert.Equal(t, secondNodeID, cachedVal2.NodeIDs[0])

	})

	// --- Test Case 4: Search with No Results (PERSON, "Unknown") ---
	t.Run("Search No Results", func(t *testing.T) {
		searchReq := &network.SearchNodesRequest{
			Type: nodeTypePtr(network.NodeType_PERSON), // Pass pointer
			// Use Criteria map
			Criteria: map[string]string{"name": "UnknownKeyword"},
			Limit:    func(i int32) *int32 { return &i }(10),
			Offset:   func(i int32) *int32 { return &i }(0),
		}
		cacheKey := generateSearchNodesCacheKeyForTest(searchReq)

		nodes, total, err := testRepo.SearchNodes(ctx, searchReq)
		require.NoError(t, err)
		assert.EqualValues(t, 0, total, "Expected 0 total results")
		assert.Len(t, nodes, 0, "Expected 0 nodes in results")

		// Verify empty placeholder caching
		time.Sleep(50 * time.Millisecond)
		cachedData, err := testCache.Get(ctx, cacheKey)
		require.NoError(t, err, "Failed to get data from cache after empty search")
		assert.Equal(t, []byte(neo4jrepo.SearchEmptyPlaceholder), cachedData, "Cache should contain empty placeholder")

		// Search again (Cache Hit for empty)
		nodesHit, totalHit, errHit := testRepo.SearchNodes(ctx, searchReq)
		require.NoError(t, errHit)
		assert.EqualValues(t, 0, totalHit, "Expected 0 total results (empty cache hit)")
		assert.Len(t, nodesHit, 0, "Expected 0 nodes in results (empty cache hit)")

	})

	// --- Test Case 5: Search Different Node Type (COMPANY, "Corp") ---
	t.Run("Search Different Type", func(t *testing.T) {
		searchReq := &network.SearchNodesRequest{
			Type: nodeTypePtr(network.NodeType_COMPANY), // Pass pointer
			// Use Criteria map
			Criteria: map[string]string{"name": "Alpha Corp"},
			Limit:    func(i int32) *int32 { return &i }(10),
			Offset:   func(i int32) *int32 { return &i }(0),
		}
		nodes, total, err := testRepo.SearchNodes(ctx, searchReq)
		require.NoError(t, err)
		assert.EqualValues(t, 1, total)
		require.Len(t, nodes, 1)
		assert.Equal(t, "search-c1", nodes[0].ID)
		assert.Equal(t, "Alpha Corp", nodes[0].Name)
	})
}

// --- Integration Test for GetNetwork ---

// Helper to find node by ID in a slice
func findNodeByID(nodes []*network.Node, id string) *network.Node {
	for _, n := range nodes {
		if n.ID == id {
			return n
		}
	}
	return nil
}

// Helper to find relation by ID in a slice
func findRelationByID(relations []*network.Relation, id string) *network.Relation {
	for _, r := range relations {
		if r.ID == id {
			return r
		}
	}
	return nil
}

// Helper function to generate cache key for GetNetwork (mirroring repo logic)
func generateGetNetworkCacheKeyForTest(req *network.GetNetworkRequest, maxDepth int32, limit, offset int64) string {
	// Mirror the logic from neo4jNodeRepo.generateGetNetworkCacheKey (updated)
	criteriaKeys := make([]string, 0, len(req.StartNodeCriteria))
	for k := range req.StartNodeCriteria {
		criteriaKeys = append(criteriaKeys, k)
	}
	sort.Strings(criteriaKeys)

	var criteriaBuilder strings.Builder
	for i, k := range criteriaKeys {
		if i > 0 {
			criteriaBuilder.WriteString("|")
		}
		criteriaBuilder.WriteString(k)
		criteriaBuilder.WriteString("=")
		criteriaBuilder.WriteString(req.StartNodeCriteria[k])
	}
	criteriaStr := criteriaBuilder.String()

	relTypesStr := make([]string, 0, len(req.RelationTypes))
	if req.IsSetRelationTypes() {
		for _, rt := range req.RelationTypes {
			relTypesStr = append(relTypesStr, rt.String())
		}
	}
	sort.Strings(relTypesStr)
	relTypesKeyPart := strings.Join(relTypesStr, ",")

	nodeTypesStr := make([]string, 0, len(req.NodeTypes))
	if req.IsSetNodeTypes() {
		for _, nt := range req.NodeTypes {
			nodeTypesStr = append(nodeTypesStr, nt.String())
		}
	}
	sort.Strings(nodeTypesStr)
	nodeTypesKeyPart := strings.Join(nodeTypesStr, ",")

	hasher := sha1.New()
	hasher.Write([]byte(criteriaStr))
	hasher.Write([]byte(relTypesKeyPart))
	hasher.Write([]byte(nodeTypesKeyPart))
	combinedHash := hex.EncodeToString(hasher.Sum(nil))

	return fmt.Sprintf("%s%s:%d:%d:%d", neo4jrepo.GetNetworkCachePrefix, combinedHash, maxDepth, limit, offset)
}

// Define a local struct matching the unexported one for unmarshalling cache data
type getNetworkCacheValueForTest struct {
	NodeIDs     []string `json:"node_ids"`
	RelationIDs []string `json:"relation_ids"`
}

func TestGetNetwork_Integration(t *testing.T) {
	ctx := context.Background()
	require.NotNil(t, testRepo, "Repository should be initialized")
	require.NotNil(t, testCache, "Cache should be initialized")
	require.NotNil(t, redisClient, "redisClient should be initialized") // Ensure redisClient is available
	clearTestData(ctx)                                                  // <<< Added: Ensure clean state for this test function

	// Setup initial data (as TestMain clears everything once)
	p1 := &network.Node{ID: "net-p1", Type: network.NodeType_PERSON, Name: "Net Alice", Profession: func(s string) *string { return &s }("Engineer"), Properties: map[string]string{"city": "A"}}
	p2 := &network.Node{ID: "net-p2", Type: network.NodeType_PERSON, Name: "Net Bob", Profession: func(s string) *string { return &s }("Engineer"), Properties: map[string]string{"city": "B"}}
	p3 := &network.Node{ID: "net-p3", Type: network.NodeType_PERSON, Name: "Net Charlie", Profession: func(s string) *string { return &s }("Manager"), Properties: map[string]string{"city": "A"}}
	c1 := &network.Node{ID: "net-c1", Type: network.NodeType_COMPANY, Name: "Net Corp", Properties: map[string]string{"industry": "Tech"}}
	require.NoError(t, createNodeDirectly(ctx, p1))
	require.NoError(t, createNodeDirectly(ctx, p2))
	require.NoError(t, createNodeDirectly(ctx, p3))
	require.NoError(t, createNodeDirectly(ctx, c1))

	r1 := &network.Relation{ID: "net-r1", Source: p1.ID, Target: c1.ID, Type: network.RelationType_COLLEAGUE}
	r2 := &network.Relation{ID: "net-r2", Source: p2.ID, Target: p3.ID, Type: network.RelationType_FRIEND}     // B->C
	r3 := &network.Relation{ID: "net-r3", Source: p2.ID, Target: c1.ID, Type: network.RelationType_VISITED}    // B->Corp
	r4 := &network.Relation{ID: "net-r4", Source: p3.ID, Target: p1.ID, Type: network.RelationType_SCHOOLMATE} // C->A
	r5 := &network.Relation{ID: "net-r5", Source: c1.ID, Target: p2.ID, Type: network.RelationType_FOLLOWING}  // Corp->B
	require.NoError(t, createRelationDirectly(ctx, p1.ID, c1.ID, r1))
	require.NoError(t, createRelationDirectly(ctx, p2.ID, p3.ID, r2))
	require.NoError(t, createRelationDirectly(ctx, p2.ID, c1.ID, r3))
	require.NoError(t, createRelationDirectly(ctx, p3.ID, p1.ID, r4))
	require.NoError(t, createRelationDirectly(ctx, c1.ID, p2.ID, r5))

	// --- Test Case: Depth 0 --- (Modified)
	t.Run("Get_Network_Depth_0", func(t *testing.T) {
		clearRedisCache(ctx) // Clear cache before test
		req := &network.GetNetworkRequest{
			StartNodeCriteria: map[string]string{"profession": "Engineer"},
			Depth:             0, // Explicitly set depth to 0
		}
		nodes, relations, err := testRepo.GetNetwork(ctx, req)

		// Assertions for Depth 0
		assert.NoError(t, err, "GetNetwork(Depth 0) failed")
		require.NotNil(t, nodes, "Nodes should not be nil for Depth 0")
		assert.Len(t, nodes, 2, "Expected 2 start nodes (p1, p2) for Depth 0") // Only p1 and p2 match criteria
		// Ensure relations are empty (or nil depending on impl)
		if relations == nil {
			assert.Nil(t, relations, "Expected nil relations for Depth 0")
		} else {
			assert.Len(t, relations, 0, "Expected 0 relations for Depth 0")
		}

		// Verify the correct nodes (p1, p2)
		foundP1 := false
		foundP2 := false
		for _, n := range nodes {
			if n.ID == p1.ID {
				foundP1 = true
			}
			if n.ID == p2.ID {
				foundP2 = true
			}
		}
		assert.True(t, foundP1, "Start node p1 should be found for Depth 0")
		assert.True(t, foundP2, "Start node p2 should be found for Depth 0")
	})

	// --- Test Case: Depth 1 (Cache Miss) --- (Modified)
	t.Run("Get_Network_Depth_1_Cache_Miss", func(t *testing.T) {
		clearRedisCache(ctx) // Clear cache before test
		req := &network.GetNetworkRequest{
			StartNodeCriteria: map[string]string{"profession": "Engineer"},
			Depth:             1, // Explicitly set depth to 1
		}
		nodes, relations, err := testRepo.GetNetwork(ctx, req)

		// Assertions for Depth 1
		require.NoError(t, err, "GetNetwork(Depth 1) failed")
		require.NotNil(t, nodes, "Nodes should not be nil for Depth 1")
		require.NotNil(t, relations, "Relations should not be nil for Depth 1")
		// Expected nodes at depth 1 from p1, p2: p1, p2, c1 (from p1), p3 (from p2)
		assert.Len(t, nodes, 4, "Expected 4 nodes (p1, p2, p3, c1) for Depth 1")
		// Expected relations connecting these nodes: r1, r2, r3, r4, r5
		assert.Len(t, relations, 5, "Expected 5 relations (r1, r2, r3, r4, r5) for Depth 1")

		foundNodes := make(map[string]bool)
		for _, n := range nodes {
			foundNodes[n.ID] = true
		}
		assert.Contains(t, foundNodes, p1.ID)
		assert.Contains(t, foundNodes, p2.ID)
		assert.Contains(t, foundNodes, p3.ID) // Neighbor of p2
		assert.Contains(t, foundNodes, c1.ID) // Neighbor of p1

		foundRels := make(map[string]bool)
		for _, r := range relations {
			foundRels[r.ID] = true
		}
		assert.Contains(t, foundRels, r1.ID) // p1 -> c1
		assert.Contains(t, foundRels, r2.ID) // p2 -> p3
		assert.Contains(t, foundRels, r3.ID) // p2 -> c1 (VISITED)
		assert.Contains(t, foundRels, r4.ID) // p3 -> p1
		assert.Contains(t, foundRels, r5.ID) // c1 -> p2

		// Check cache status after miss
		// Attempt to GetNode for p1, should hit cache now
		time.Sleep(50 * time.Millisecond) // Short delay to allow potential async cache write
		cachedP1, getNodeErr := testRepo.GetNode(ctx, p1.ID)
		assert.NoError(t, getNodeErr, "GetNode for p1 after GetNetwork should succeed")
		assert.NotNil(t, cachedP1, "GetNode for p1 after GetNetwork should return node")
		// Check a log message or metric if possible to confirm cache hit for GetNode
	})

	// --- Test Case: Depth 1 (Cache Hit) --- (Modified)
	t.Run("Get_Network_Depth_1_Cache_Hit", func(t *testing.T) {
		fmt.Println("Expecting GetNetwork cache hit...")
		// Ensure data is in cache from previous Cache Miss subtest (DON'T clear cache here)
		req := &network.GetNetworkRequest{
			StartNodeCriteria: map[string]string{"profession": "Engineer"},
			Depth:             1, // Depth 1
		}

		// Call GetNetwork again
		nodes, relations, err := testRepo.GetNetwork(ctx, req)

		require.NoError(t, err, "GetNetwork(Depth 1, Cache Hit) failed")
		require.NotNil(t, nodes, "Nodes should not be nil (cache hit)")
		require.NotNil(t, relations, "Relations should not be nil (cache hit)")
		assert.Len(t, nodes, 4, "Expected 4 nodes (cache hit)")
		assert.Len(t, relations, 5, "Expected 5 relations (cache hit)")

		// Optional: Verify content matches previous test
		// ...
	})

	// --- Test Case: Invalid Depth (< 0) --- (Modified)
	t.Run("Get_Network_Invalid_Depth", func(t *testing.T) {
		req := &network.GetNetworkRequest{
			StartNodeCriteria: map[string]string{"profession": "Engineer"},
			Depth:             -1, // Invalid depth
		}
		_, _, err := testRepo.GetNetwork(ctx, req)
		assert.ErrorIs(t, err, neo4jrepo.ErrInvalidDepth, "Expected ErrInvalidDepth for negative depth") // Use exported error
	})
}

// --- Integration Test for GetPath ---

// Helper function to generate cache key for GetPath (mirroring repo logic)
func generateGetPathCacheKeyForTest(req *network.GetPathRequest, maxDepth int32, relationTypesStr []string) string {
	// Mirror the logic from neo4jNodeRepo.generateGetPathCacheKey
	sortedTypes := make([]string, len(relationTypesStr))
	copy(sortedTypes, relationTypesStr)
	sort.Strings(sortedTypes)
	typesKeyPart := strings.Join(sortedTypes, ",")

	hasher := sha1.New()
	hasher.Write([]byte(typesKeyPart))
	typesHash := hex.EncodeToString(hasher.Sum(nil))

	return fmt.Sprintf("%s%s:%s:%d:%s", neo4jrepo.GetPathCachePrefix, req.SourceID, req.TargetID, maxDepth, typesHash)
}

// Define a local struct matching the unexported one for unmarshalling cache data
type getPathCacheValueForTest struct {
	NodeIDs     []string `json:"node_ids"`
	RelationIDs []string `json:"relation_ids"`
}

func TestGetPath_Integration(t *testing.T) {
	ctx := context.Background()
	require.NotNil(t, testRepo, "Repository should be initialized")
	require.NotNil(t, relTestRelRepo, "RelationRepository should be initialized")
	require.NotNil(t, testCache, "Cache should be initialized")
	clearTestData(ctx)

	// 1. Setup: Create a path
	// A -(FRIEND)-> B -(COLLEAGUE)-> C -(SCHOOLMATE)-> D
	nA := &network.Node{ID: "path-a", Type: network.NodeType_PERSON, Name: "Path A"}
	nB := &network.Node{ID: "path-b", Type: network.NodeType_PERSON, Name: "Path B"}
	nC := &network.Node{ID: "path-c", Type: network.NodeType_PERSON, Name: "Path C"}
	nD := &network.Node{ID: "path-d", Type: network.NodeType_PERSON, Name: "Path D"}
	nE := &network.Node{ID: "path-e", Type: network.NodeType_PERSON, Name: "Path E"} // Unconnected

	nodesToCreate := []*network.Node{nA, nB, nC, nD, nE}
	for _, node := range nodesToCreate {
		err := createNodeDirectly(ctx, node)
		require.NoError(t, err, "Failed to create node for path test: %s", node.ID)
	}

	rAB := &network.Relation{ID: "path-rab", Type: network.RelationType_FRIEND}
	rBC := &network.Relation{ID: "path-rbc", Type: network.RelationType_COLLEAGUE}
	rCD := &network.Relation{ID: "path-rcd", Type: network.RelationType_SCHOOLMATE}
	rAC := &network.Relation{ID: "path-rac", Type: network.RelationType_VISITED} // Alternate path A->C

	require.NoError(t, createRelationDirectly(ctx, nA.ID, nB.ID, rAB))
	require.NoError(t, createRelationDirectly(ctx, nB.ID, nC.ID, rBC))
	require.NoError(t, createRelationDirectly(ctx, nC.ID, nD.ID, rCD))
	require.NoError(t, createRelationDirectly(ctx, nA.ID, nC.ID, rAC))

	// Pre-cache B and rBC
	_, _ = testRepo.GetNode(ctx, nB.ID)
	_, _ = relTestRelRepo.GetRelation(ctx, rBC.ID)
	time.Sleep(50 * time.Millisecond) // Allow cache writes

	// --- Test Case 1: Find shortest path A->D (default depth, no type filter) - Cache Miss ---
	t.Run("Get Shortest Path Cache Miss", func(t *testing.T) {
		req := &network.GetPathRequest{
			SourceID: nA.ID,
			TargetID: nD.ID,
			// MaxDepth: default (3)
			// Types: nil (no filter)
		}
		// Default maxDepth is 3, no types
		cacheKey := generateGetPathCacheKeyForTest(req, 3, []string{})

		// Verify cache miss
		_, err := testCache.Get(ctx, cacheKey)
		assert.ErrorIs(t, err, cache.ErrNotFound, "Cache should be empty before first GetPath")

		// Execute GetPath
		nodes, relations, err := testRepo.GetPath(ctx, req)
		require.NoError(t, err, "GetPath failed for A->D")

		// Verify results (Shortest path is now A->C->D due to direct A->C link)
		require.Len(t, nodes, 3, "Expected 3 nodes in path A->C->D")
		assert.Equal(t, nA.ID, nodes[0].ID)
		assert.Equal(t, nC.ID, nodes[1].ID) // Expect C directly after A
		assert.Equal(t, nD.ID, nodes[2].ID) // Expect D after C

		require.Len(t, relations, 2, "Expected 2 relations in path A->C->D")
		assert.Equal(t, rAC.ID, relations[0].ID) // Expect rAC
		assert.Equal(t, rCD.ID, relations[1].ID) // Expect rCD

		// Verify cache population
		time.Sleep(50 * time.Millisecond)
		cachedData, err := testCache.Get(ctx, cacheKey)
		require.NoError(t, err, "Failed to get data from cache after GetPath")
		require.NotEmpty(t, cachedData, "Cache should contain data after GetPath")

		// Verify cache content (IDs in order)
		var cachedValue getPathCacheValueForTest
		err = json.Unmarshal(cachedData, &cachedValue)
		require.NoError(t, err, "Failed to unmarshal cached path data")
		assert.Equal(t, []string{nA.ID, nC.ID, nD.ID}, cachedValue.NodeIDs) // Adjusted expected nodes
		assert.Equal(t, []string{rAC.ID, rCD.ID}, cachedValue.RelationIDs)  // Adjusted expected relations

		// Also verify relation details are cached
		relDetailAB, errRelCacheAB := relTestRelRepo.GetRelation(ctx, rAB.ID)
		relDetailBC, errRelCacheBC := relTestRelRepo.GetRelation(ctx, rBC.ID)
		assert.NoError(t, errRelCacheAB, "Relation AB detail cache check failed")
		assert.NoError(t, errRelCacheBC, "Relation BC detail cache check failed")
		assert.NotNil(t, relDetailAB, "Relation AB detail should be cached")
		assert.NotNil(t, relDetailBC, "Relation BC detail should be cached")
	})

	// --- Test Case 2: Find shortest path A->D - Cache Hit ---
	t.Run("Get Shortest Path Cache Hit", func(t *testing.T) {
		req := &network.GetPathRequest{
			SourceID: nA.ID,
			TargetID: nD.ID,
		}
		cacheKey := generateGetPathCacheKeyForTest(req, 3, []string{})

		// Ensure cache exists
		_, err := testCache.Get(ctx, cacheKey)
		require.NoError(t, err, "Cache should exist for hit test")

		// Execute GetPath again
		t.Log("Expecting GetPath cache hit...")
		nodes, relations, err := testRepo.GetPath(ctx, req)
		require.NoError(t, err, "GetPath (cache hit) failed for A->D")

		// Verify results again (should use cached IDs and fetch details)
		require.Len(t, nodes, 3)                 // Adjusted expectation
		require.Len(t, relations, 2)             // Adjusted expectation
		assert.Equal(t, nC.ID, nodes[1].ID)      // Adjusted expectation
		assert.Equal(t, rCD.ID, relations[1].ID) // Adjusted expectation
		// Check details of a node/relation NOT pre-cached this time (e.g., nC, rCD)
		nodeC := findNodeByID(nodes, nC.ID)
		require.NotNil(t, nodeC)
		assert.Equal(t, "Path C", nodeC.Name)
		relCD := findRelationByID(relations, rCD.ID)
		require.NotNil(t, relCD)
		assert.Equal(t, network.RelationType_SCHOOLMATE, relCD.Type)

		// Ensure relation details are still retrieved (potentially from cache)
		relDetailAB, errRelCacheAB := relTestRelRepo.GetRelation(ctx, rAB.ID)
		assert.NoError(t, errRelCacheAB, "Relation AB detail cache check failed (cache hit)")
		assert.NotNil(t, relDetailAB, "Relation AB detail should be retrieved (cache hit)")
	})

	// --- Test Case 3: Find path A->C (MaxDepth 1, Type VISITED) ---
	t.Run("Get Path A->C Depth 1 VISITED", func(t *testing.T) {
		depth := int32(1)
		types := []network.RelationType{network.RelationType_VISITED}
		typesStr := []string{network.RelationType_VISITED.String()}
		req := &network.GetPathRequest{
			SourceID: nA.ID,
			TargetID: nC.ID,
			MaxDepth: &depth,
			Types:    types,
		}
		cacheKey := generateGetPathCacheKeyForTest(req, depth, typesStr)

		// Execute GetPath
		nodes, relations, err := testRepo.GetPath(ctx, req)
		require.NoError(t, err, "GetPath failed for A->C (Depth 1, VISITED)")

		// Verify results (A -[VISITED]-> C)
		require.Len(t, nodes, 2, "Expected 2 nodes")
		assert.Equal(t, nA.ID, nodes[0].ID)
		assert.Equal(t, nC.ID, nodes[1].ID)
		require.Len(t, relations, 1, "Expected 1 relation")
		assert.Equal(t, rAC.ID, relations[0].ID)
		assert.Equal(t, network.RelationType_VISITED, relations[0].Type)

		// Verify cache population
		time.Sleep(50 * time.Millisecond)
		cachedData, err := testCache.Get(ctx, cacheKey)
		require.NoError(t, err)
		var cachedValue getPathCacheValueForTest
		require.NoError(t, json.Unmarshal(cachedData, &cachedValue))
		assert.Equal(t, []string{nA.ID, nC.ID}, cachedValue.NodeIDs)
		assert.Equal(t, []string{rAC.ID}, cachedValue.RelationIDs)
	})

	// --- Test Case 4: Find path A->D (MaxDepth 2) - Path Found Within Limit ---
	t.Run("Get Path A->D MaxDepth 2 Path Found", func(t *testing.T) { // Renamed for clarity
		depth := int32(2)
		req := &network.GetPathRequest{
			SourceID: nA.ID,
			TargetID: nD.ID,
			MaxDepth: &depth,
		}
		cacheKey := generateGetPathCacheKeyForTest(req, depth, []string{})

		// Execute GetPath
		// Expect NoError because path A->C->D (2 relations) IS within maxDepth 2.
		nodes, relations, err := testRepo.GetPath(ctx, req)
		require.NoError(t, err, "GetPath failed for A->D within MaxDepth 2")

		// Verify results (A->C->D)
		require.Len(t, nodes, 3, "Expected 3 nodes in path A->C->D")
		assert.Equal(t, nA.ID, nodes[0].ID)
		assert.Equal(t, nC.ID, nodes[1].ID)
		assert.Equal(t, nD.ID, nodes[2].ID)
		require.Len(t, relations, 2, "Expected 2 relations in path A->C->D")
		assert.Equal(t, rAC.ID, relations[0].ID)
		assert.Equal(t, rCD.ID, relations[1].ID)

		// Verify path WAS cached correctly
		time.Sleep(50 * time.Millisecond)
		cachedData, errCacheGet := testCache.Get(ctx, cacheKey)
		require.NoError(t, errCacheGet, "Cache should contain data for path found within depth")
		var cachedValue getPathCacheValueForTest
		require.NoError(t, json.Unmarshal(cachedData, &cachedValue), "Failed to unmarshal cached path data")
		assert.Equal(t, []string{nA.ID, nC.ID, nD.ID}, cachedValue.NodeIDs)
		assert.Equal(t, []string{rAC.ID, rCD.ID}, cachedValue.RelationIDs)

		// Execute again (cache hit)
		nodesHit, relationsHit, errHit := testRepo.GetPath(ctx, req)
		require.NoError(t, errHit, "Expected NoError on second call (cache hit)")
		require.Len(t, nodesHit, 3)
		require.Len(t, relationsHit, 2)
		assert.Equal(t, nD.ID, nodesHit[2].ID)
	})

	// --- Test Case 5: Find path A->E (Unconnected Node) - Path Not Found ---
	t.Run("Get Path A->E Not Found", func(t *testing.T) {
		req := &network.GetPathRequest{
			SourceID: nA.ID,
			TargetID: nE.ID, // Unconnected node
		}
		cacheKey := generateGetPathCacheKeyForTest(req, 3, []string{})

		// Execute GetPath
		nodes, relations, err := testRepo.GetPath(ctx, req)
		require.Error(t, err, "Expected an error for path not found to unconnected node")
		// assert.True(t, isNotFoundError(err), "Error should be a 'not found' error")
		assert.Nil(t, nodes)
		assert.Nil(t, relations)

		// Verify empty placeholder caching
		time.Sleep(50 * time.Millisecond)
		cachedData, errCacheGet := testCache.Get(ctx, cacheKey)
		require.NoError(t, errCacheGet, "Get on cache key for non-existent path failed unexpectedly")
		assert.Equal(t, []byte(neo4jrepo.GetPathEmptyPlaceholder), cachedData)

		// Execute again (cache hit for empty)
		nodesHit, relationsHit, errHit := testRepo.GetPath(ctx, req)
		require.Error(t, errHit, "Expected error on cache hit for empty path (unconnected)")
		assert.Nil(t, nodesHit)
		assert.Nil(t, relationsHit)
	})

	// --- Test Case 6: Find path A->D (Type FRIEND|COLLEAGUE) ---
	t.Run("Get Path A->D Type Filter FRIEND_COLLEAGUE", func(t *testing.T) {
		types := []network.RelationType{network.RelationType_FRIEND, network.RelationType_COLLEAGUE}
		typesStr := []string{network.RelationType_FRIEND.String(), network.RelationType_COLLEAGUE.String()}
		req := &network.GetPathRequest{
			SourceID: nA.ID,
			TargetID: nD.ID, // D is only reachable via SCHOOLMATE from C
			Types:    types,
		}
		cacheKey := generateGetPathCacheKeyForTest(req, 3, typesStr)

		// Execute GetPath
		nodes, relations, err := testRepo.GetPath(ctx, req)
		require.Error(t, err, "Expected an error for path not found with type filter")
		// assert.True(t, isNotFoundError(err), "Error should be a 'not found' error")
		assert.Nil(t, nodes)
		assert.Nil(t, relations)

		// Verify empty placeholder caching
		time.Sleep(50 * time.Millisecond)
		cachedData, errCacheGet := testCache.Get(ctx, cacheKey)
		require.NoError(t, errCacheGet, "Get on cache key for type-filtered non-existent path failed unexpectedly")
		assert.Equal(t, []byte(neo4jrepo.GetPathEmptyPlaceholder), cachedData)

		// Execute again (cache hit for empty)
		nodesHit, relationsHit, errHit := testRepo.GetPath(ctx, req)
		require.Error(t, errHit, "Expected error on cache hit for empty path (type filter)")
		assert.Nil(t, nodesHit)
		assert.Nil(t, relationsHit)
	})

}
