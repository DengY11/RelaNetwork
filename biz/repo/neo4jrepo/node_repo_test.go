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
	neo4jURI := os.Getenv("NEO4J_URI")   // e.g., "neo4j://localhost:7687"
	neo4jUser := os.Getenv("NEO4J_USER") // e.g., "neo4j"
	neo4jPass := os.Getenv("NEO4J_PASS") // e.g., "password"
	if neo4jURI == "" {
		neo4jURI = "neo4j://localhost:7687" // Default fallback
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
	redisAddr := os.Getenv("REDIS_ADDR") // e.g., "localhost:6379"
	if redisAddr == "" {
		redisAddr = "localhost:6379" // Default fallback
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

	// --- Setup Repository ---
	// We need a real RelationRepository implementation or a minimal mock/stub if its methods aren't directly tested here
	// For now, let's assume a minimal stub or nil if NodeRepository methods don't strictly require it.
	// If RelationRepository methods *are* needed (like in GetNetwork tests), you'll need a real one too.
	var dummyRelRepo neo4jrepo.RelationRepository                                    // Replace with real or functional stub if needed
	testRepo = neo4jrepo.NewNodeRepository(testDriver, nil, testCache, dummyRelRepo) // DAL is not used directly by repo anymore, pass nil or real DAL if needed by underlying calls

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
			rdbErr := redisClient.Del(ctx, keys...).Err()
			if rdbErr != nil {
				fmt.Printf("Warning: Failed to clear Redis keys with prefix %s: %v\n", prefix, rdbErr)
			}
		} else if err != nil && !errors.Is(err, redis.Nil) {
			fmt.Printf("Warning: Failed to get Redis keys with prefix %s: %v\n", prefix, err)
		}
	}
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

// --- Add more integration tests for UpdateNode, DeleteNode, SearchNodes etc. ---
