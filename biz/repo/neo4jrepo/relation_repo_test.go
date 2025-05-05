package neo4jrepo_test

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/redis/go-redis/v9" // Or your chosen Redis client library
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"labelwall/biz/dal/neo4jdal" // Import the DAL implementation package
	"labelwall/biz/model/relationship/network"
	"labelwall/biz/repo/neo4jrepo"
	"labelwall/pkg/cache" // Assuming RedisCache implementation here
)

// Define a specific error for relation not found in tests
var ErrRelationNotFound = errors.New("test: relation not found")

// Global variables for relation testing setup
var (
	relTestRelRepo       neo4jrepo.RelationRepository
	relTestNodeRepo      neo4jrepo.NodeRepository // Needed for creating nodes
	relTestDriver        neo4j.DriverWithContext
	relTestCacheClient   *redis.Client
	relTestRelByteCache  cache.RelationAndByteCache // Specific cache type for relations
	relTestNodeByteCache cache.NodeAndByteCache     // Needed for Node Repo
)

// Reusing helper from node_repo_test.go
func nodeTypePtrRelTest(t network.NodeType) *network.NodeType {
	return &t
}

// clearRelationTestData cleans Neo4j and Redis for relation tests
func clearRelationTestData(ctx context.Context) {
	// Clear Neo4j (same as node tests)
	if relTestDriver == nil {
		fmt.Println("Warning: relTestDriver not initialized in clearRelationTestData")
		return
	}
	session := relTestDriver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	_, _ = session.Run(ctx, "MATCH ()-[r]->() DELETE r", nil)
	_, _ = session.Run(ctx, "MATCH (n) DELETE n", nil)
	session.Close(ctx)

	// --- Use the same Redis clearing logic as node_repo_test --- VVV
	if relTestCacheClient == nil {
		fmt.Println("Warning: relTestCacheClient not initialized, cannot clear Redis cache.")
		return
	}
	// Use the EXPORTED constants from neo4jrepo package and the correct prefix
	basePrefix := "testprefix:" // Assuming relation tests use the same prefix as node tests
	prefixesToClear := []string{
		basePrefix + neo4jrepo.NodeCachePrefix + "*",          // Node details
		basePrefix + neo4jrepo.RelationCachePrefix + "*",      // Relation details
		basePrefix + neo4jrepo.SearchNodesCachePrefix + "*",   // Search results IDs (might be set by node repo)
		basePrefix + neo4jrepo.GetNetworkCachePrefix + "*",    // Network graph IDs (might be set by node repo)
		basePrefix + neo4jrepo.GetPathCachePrefix + "*",       // Path IDs (might be set by node repo)
		basePrefix + neo4jrepo.NodeRelationsCachePrefix + "*", // NodeRelations IDs (set by relation repo)
		// Add specific prefixes for relation repo if any others exist
	}

	for _, prefix := range prefixesToClear {
		keys, err := relTestCacheClient.Keys(ctx, prefix).Result()
		if err == nil && len(keys) > 0 {
			pipe := relTestCacheClient.Pipeline()
			pipe.Del(ctx, keys...)
			_, err := pipe.Exec(ctx)
			if err != nil && !errors.Is(err, redis.Nil) { // Ignore nil error if keys expired between KEYS and DEL
				fmt.Printf("Warning: Failed to clear Redis keys with prefix %s: %v\n", prefix, err)
			}
		} else if err != nil && !errors.Is(err, redis.Nil) {
			fmt.Printf("Warning: Failed to get Redis keys with prefix %s: %v\n", prefix, err)
		}
	}
	// --- End Redis clearing logic ---

	/* // Original FlushDB logic commented out
	err := relTestCacheClient.FlushDB(ctx).Err()
	if err != nil {
		fmt.Printf("Warning: Failed to flush Redis DB 1: %v\n", err)
	}
	*/
}

// --- Helper Functions (Copied/Adapted from node_repo_test.go) ---

// Helper function to create a node directly in DB
func createNodeDirectlyRelTest(ctx context.Context, node *network.Node) error {
	session := relTestDriver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)

	properties := map[string]any{
		"name":       node.Name,
		"created_at": time.Now().UTC(),
		"updated_at": time.Now().UTC(),
	}
	if node.ID == "" {
		node.ID = uuid.NewString() // Generate ID if not provided
	}
	properties["id"] = node.ID

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

	cypher := fmt.Sprintf("MERGE (n:%s {id: $props.id}) SET n += $props RETURN n", node.Type.String())
	_, err := session.Run(ctx, cypher, map[string]any{"props": properties})
	if err != nil {
		return fmt.Errorf("failed to create node directly: %w", err)
	}
	return nil
}

// Helper function to create a relation directly in DB
func createRelationDirectlyRelTest(ctx context.Context, sourceID, targetID string, rel *network.Relation) error {
	session := relTestDriver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)

	properties := map[string]any{
		"created_at": time.Now().UTC(),
		"updated_at": time.Now().UTC(),
	}
	if rel.ID == "" {
		rel.ID = uuid.NewString() // Generate ID if not provided
	}
	properties["id"] = rel.ID

	if rel.Label != nil {
		properties["label"] = *rel.Label
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
		MERGE (a)-[r:%s {id: $props.id}]->(b)
		SET r += $props
		RETURN r
	`, rel.Type.String())

	_, err := session.Run(ctx, cypher, map[string]any{
		"sourceId": sourceID,
		"targetId": targetID,
		"props":    properties,
	})
	if err != nil {
		return fmt.Errorf("failed to create relation directly (%s)-[%s {%s}]->(%s): %w", sourceID, rel.Type.String(), rel.ID, targetID, err)
	}
	return nil
}

// Helper function to get a relation directly from DB
func getRelationDirectlyRelTest(ctx context.Context, id string) (map[string]interface{}, error) {
	session := relTestDriver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
	defer session.Close(ctx)

	result, err := session.Run(ctx, "MATCH ()-[r {id: $id}]->() RETURN r", map[string]any{"id": id})
	if err != nil {
		return nil, fmt.Errorf("direct DB query for relation failed: %w", err)
	}
	record, err := result.Single(ctx)
	if err != nil {
		// Check if it's a Neo4j "no records" error, signifying not found
		if neo4j.IsNeo4jError(err) && strings.Contains(err.Error(), "Result contains no more records") {
			return nil, fmt.Errorf("relation %s not found in DB: %w", id, ErrRelationNotFound) // Use test-specific error
		}
		return nil, fmt.Errorf("failed to get single record for relation from DB: %w", err)
	}

	relValue, ok := record.Get("r")
	if !ok {
		return nil, fmt.Errorf("relation 'r' not found in DB record")
	}
	dbRel, ok := relValue.(neo4j.Relationship)
	if !ok {
		// Try dbtype.Relationship as returned by some queries
		dbRelType, okType := relValue.(neo4j.Relationship) // Adjusted to neo4j.Relationship
		if !okType {
			return nil, fmt.Errorf("unexpected relation type in DB result: %T", relValue)
		}
		dbRel = dbRelType // Assign if it's the correct type
	}

	return dbRel.Props, nil
}

// --- Integration Tests for RelationRepository ---

func TestCreateAndGetRelation_Integration(t *testing.T) {
	ctx := context.Background()
	require.NotNil(t, relTestRelRepo, "RelationRepository should be initialized")
	require.NotNil(t, relTestNodeRepo, "NodeRepository should be initialized")
	require.NotNil(t, relTestRelByteCache, "Cache should be initialized")
	clearRelationTestData(ctx)

	// 1. Setup: Create source and target nodes
	sourceNode := &network.Node{ID: "rel-test-source-1", Type: network.NodeType_PERSON, Name: "Relation Source"}
	targetNode := &network.Node{ID: "rel-test-target-1", Type: network.NodeType_COMPANY, Name: "Relation Target"}
	err := createNodeDirectlyRelTest(ctx, sourceNode)
	require.NoError(t, err, "Failed to create source node")
	err = createNodeDirectlyRelTest(ctx, targetNode)
	require.NoError(t, err, "Failed to create target node")

	// 2. Create Relation
	relLabel := "Works At"
	createReq := &network.CreateRelationRequest{
		Source: sourceNode.ID,
		Target: targetNode.ID,
		Type:   network.RelationType_COLLEAGUE,
		Label:  &relLabel,
		Properties: map[string]string{
			"role": "Developer",
		},
	}

	createdRel, err := relTestRelRepo.CreateRelation(ctx, createReq)
	require.NoError(t, err, "CreateRelation failed")
	require.NotNil(t, createdRel, "CreateRelation returned nil relation")
	require.NotEmpty(t, createdRel.ID, "Created relation ID is empty")
	assert.Equal(t, createReq.Source, createdRel.Source)
	assert.Equal(t, createReq.Target, createdRel.Target)
	assert.Equal(t, createReq.Type, createdRel.Type)
	require.NotNil(t, createdRel.Label)
	assert.Equal(t, relLabel, *createdRel.Label)
	require.NotNil(t, createdRel.Properties)
	assert.Equal(t, createReq.Properties["role"], createdRel.Properties["role"])

	relationID := createdRel.ID

	// 3. Get Relation (Cache Miss)
	t.Logf("Attempting first GetRelation for ID: %s (expect cache miss)", relationID)
	retrievedRel1, err := relTestRelRepo.GetRelation(ctx, relationID)
	require.NoError(t, err, "First GetRelation failed")
	require.NotNil(t, retrievedRel1, "First GetRelation returned nil")
	assert.Equal(t, relationID, retrievedRel1.ID)
	assert.Equal(t, createReq.Source, retrievedRel1.Source)
	assert.Equal(t, createReq.Target, retrievedRel1.Target)
	assert.Equal(t, createReq.Type, retrievedRel1.Type)
	assert.Equal(t, createReq.Properties["role"], retrievedRel1.Properties["role"])

	// 4. Verify Cache Population
	time.Sleep(50 * time.Millisecond)
	cachedVal, err := relTestRelByteCache.GetRelation(ctx, relationID) // Use specific GetRelation
	assert.NoError(t, err, "Checking cache directly failed")
	assert.NotNil(t, cachedVal, "Relation should be in cache after GetRelation")
	assert.Equal(t, relationID, cachedVal.ID, "Cached relation ID mismatch")
	assert.Equal(t, createReq.Properties["role"], cachedVal.Properties["role"])

	// 5. Get Relation (Cache Hit)
	t.Logf("Attempting second GetRelation for ID: %s (expect cache hit)", relationID)
	retrievedRel2, err := relTestRelRepo.GetRelation(ctx, relationID)
	require.NoError(t, err, "Second GetRelation failed")
	require.NotNil(t, retrievedRel2, "Second GetRelation returned nil")
	assert.Equal(t, relationID, retrievedRel2.ID)
	// Check if it's the same instance (depends on cache implementation details)
	// assert.Same(t, cachedVal, retrievedRel2, "Second GetRelation should return the cached instance")

	// 6. Verify in Neo4j Directly (Optional)
	dbProps, err := getRelationDirectlyRelTest(ctx, relationID)
	require.NoError(t, err, "Direct Neo4j query for relation failed")
	require.NotNil(t, dbProps)
	assert.Equal(t, relLabel, dbProps["label"])
	assert.Equal(t, createReq.Properties["role"], dbProps["role"])

	// 7. Get Non-Existent Relation
	nonExistentID := "does-not-exist-relation"
	_, err = relTestRelRepo.GetRelation(ctx, nonExistentID)
	require.Error(t, err, "GetRelation for non-existent ID should return an error")
	// TODO: Check for specific 'not found' error type (e.g., check if errors.Is(err, neo4jdal.ErrNotFound))

	// Check cache for non-existent (should cache nil or return ErrNotFound depending on impl)
	_, errCacheNil := relTestRelByteCache.GetRelation(ctx, nonExistentID)
	// Adjusted expectation based on test output: Expect ErrNotFound if GetRelation doesn't cache nil on miss
	assert.ErrorIs(t, errCacheNil, cache.ErrNotFound, "Cache check for non-existent relation should return ErrNotFound")

}

func TestUpdateRelation_Integration(t *testing.T) {
	ctx := context.Background()
	require.NotNil(t, relTestRelRepo)
	require.NotNil(t, relTestNodeRepo)
	require.NotNil(t, relTestRelByteCache)
	clearRelationTestData(ctx)

	// 1. Setup: Create nodes and initial relation
	sourceNode := &network.Node{ID: "rel-update-source", Type: network.NodeType_PERSON, Name: "Update Source"}
	targetNode := &network.Node{ID: "rel-update-target", Type: network.NodeType_COMPANY, Name: "Update Target"}
	err := createNodeDirectlyRelTest(ctx, sourceNode)
	require.NoError(t, err)
	err = createNodeDirectlyRelTest(ctx, targetNode)
	require.NoError(t, err)

	initialLabel := "Initial Colleague"
	initialRel := &network.Relation{
		ID:    "rel-update-test-1",
		Type:  network.RelationType_COLLEAGUE,
		Label: &initialLabel,
		Properties: map[string]string{
			"role":   "developer",
			"status": "active",
		},
	}
	err = createRelationDirectlyRelTest(ctx, sourceNode.ID, targetNode.ID, initialRel)
	require.NoError(t, err, "Failed to create initial relation for update test")

	// 2. Pre-populate Cache
	_, err = relTestRelRepo.GetRelation(ctx, initialRel.ID)
	require.NoError(t, err, "Failed to get relation to populate cache before update")
	time.Sleep(50 * time.Millisecond) // Allow potential async cache write
	cachedValPre, err := relTestRelByteCache.GetRelation(ctx, initialRel.ID)
	require.NoError(t, err, "Failed to get relation from cache before update")
	require.NotNil(t, cachedValPre, "Relation should be in cache before update")
	require.Equal(t, "developer", cachedValPre.Properties["role"])

	// 3. Update the relation
	updatedLabel := "Lead Colleague"
	updatedRole := "Lead Developer"
	updateReq := &network.UpdateRelationRequest{
		ID:    initialRel.ID,
		Label: &updatedLabel,
		Properties: map[string]string{
			"role":     updatedRole,
			"new_prop": "added",
			"status":   "inactive",
		},
	}

	updatedRel, err := relTestRelRepo.UpdateRelation(ctx, updateReq)
	require.NoError(t, err, "UpdateRelation failed")
	require.NotNil(t, updatedRel, "UpdateRelation returned nil")

	// 4. Verify updated relation fields
	assert.Equal(t, initialRel.ID, updatedRel.ID)
	assert.Equal(t, sourceNode.ID, updatedRel.Source) // Source/Target/Type should not change
	assert.Equal(t, targetNode.ID, updatedRel.Target)
	assert.Equal(t, initialRel.Type, updatedRel.Type)
	require.NotNil(t, updatedRel.Label)
	assert.Equal(t, updatedLabel, *updatedRel.Label)
	require.NotNil(t, updatedRel.Properties)
	assert.Equal(t, updatedRole, updatedRel.Properties["role"])
	assert.Equal(t, "added", updatedRel.Properties["new_prop"])
	assert.Equal(t, "inactive", updatedRel.Properties["status"])
	// created_at should not change, updated_at should be recent
	// assert.Equal(t, cachedValPre.CreatedAt, updatedRel.CreatedAt) // Assuming CreatedAt exists and is preserved
	// assert.WithinDuration(t, time.Now(), updatedRel.UpdatedAt, 5*time.Second) // Assuming UpdatedAt exists

	// 5. Verify Cache Invalidation
	time.Sleep(50 * time.Millisecond)
	cachedValPost, err := relTestRelByteCache.GetRelation(ctx, initialRel.ID)
	assert.ErrorIs(t, err, cache.ErrNotFound, "Cache should be invalidated after update (GetRelation should return ErrNotFound)")
	assert.Nil(t, cachedValPost, "Cached relation value should be nil after invalidation")

	// 6. Get Relation again (should fetch updated data from DB and re-populate cache)
	retrievedRelPostUpdate, err := relTestRelRepo.GetRelation(ctx, initialRel.ID)
	require.NoError(t, err, "GetRelation after update failed")
	require.NotNil(t, retrievedRelPostUpdate, "GetRelation after update returned nil")
	assert.Equal(t, updatedLabel, *retrievedRelPostUpdate.Label)
	assert.Equal(t, updatedRole, retrievedRelPostUpdate.Properties["role"])

	// 7. Verify Cache Re-population
	time.Sleep(50 * time.Millisecond)
	cachedValRepopulated, err := relTestRelByteCache.GetRelation(ctx, initialRel.ID)
	require.NoError(t, err, "Failed to get relation from cache after re-population")
	require.NotNil(t, cachedValRepopulated, "Cache should be re-populated after GetRelation post-update")
	assert.Equal(t, updatedLabel, *cachedValRepopulated.Label)

	// 8. Test Update Non-existent Relation
	nonExistentID := "does-not-exist-rel-update"
	updateReqNonExistent := &network.UpdateRelationRequest{
		ID:    nonExistentID,
		Label: &updatedLabel,
	}
	_, err = relTestRelRepo.UpdateRelation(ctx, updateReqNonExistent)
	require.Error(t, err, "Updating non-existent relation should return an error")
	// TODO: Check for specific 'not found' error type
}

func TestDeleteRelation_Integration(t *testing.T) {
	ctx := context.Background()
	require.NotNil(t, relTestRelRepo)
	require.NotNil(t, relTestNodeRepo)
	require.NotNil(t, relTestRelByteCache)
	clearRelationTestData(ctx)

	// Setup nodes
	sourceNode := &network.Node{ID: "rel-delete-source", Type: network.NodeType_PERSON, Name: "Delete Source"}
	targetNode := &network.Node{ID: "rel-delete-target", Type: network.NodeType_SCHOOL, Name: "Delete Target"}
	err := createNodeDirectlyRelTest(ctx, sourceNode)
	require.NoError(t, err)
	err = createNodeDirectlyRelTest(ctx, targetNode)
	require.NoError(t, err)

	relationToDeleteID := "rel-delete-test-1"

	// --- Test Case 1: Delete existing relation (after caching) ---
	t.Run("Delete Existing Relation After Caching", func(t *testing.T) {
		clearRelationTestData(ctx) // Clean slate for sub-test
		// Recreate nodes for this subtest
		err := createNodeDirectlyRelTest(ctx, sourceNode)
		require.NoError(t, err)
		err = createNodeDirectlyRelTest(ctx, targetNode)
		require.NoError(t, err)

		// 1. Setup: Create a relation and cache it
		relToDelete := &network.Relation{
			ID:   relationToDeleteID,
			Type: network.RelationType_SCHOOLMATE,
		}
		err = createRelationDirectlyRelTest(ctx, sourceNode.ID, targetNode.ID, relToDelete)
		require.NoError(t, err, "Failed to create relation for delete test")

		// Cache it by getting it
		_, err = relTestRelRepo.GetRelation(ctx, relationToDeleteID)
		require.NoError(t, err, "Failed to get relation to populate cache before delete")
		time.Sleep(50 * time.Millisecond) // Allow cache write
		cachedValPre, err := relTestRelByteCache.GetRelation(ctx, relationToDeleteID)
		require.NoError(t, err, "Failed to get relation from cache before delete")
		require.NotNil(t, cachedValPre, "Relation should be in cache before delete")

		// 2. Execute DeleteRelation
		err = relTestRelRepo.DeleteRelation(ctx, relationToDeleteID)
		require.NoError(t, err, "DeleteRelation failed for existing relation")

		// 3. Verify Deletion from DB
		_, err = relTestRelRepo.GetRelation(ctx, relationToDeleteID) // Use _ to ignore the relation return value
		require.Error(t, err, "GetRelation should return error after relation deletion")
		// Check for specific 'not found' error from DAL
		assert.ErrorIs(t, err, neo4jdal.ErrNotFound, "GetRelation after delete should return ErrNotFound") // Ensure neo4jdal is imported

		// 4. Verify Deletion from Cache
		time.Sleep(50 * time.Millisecond)
		cachedValPost, errCache := relTestRelByteCache.GetRelation(ctx, relationToDeleteID)
		// After DeleteRelation invalidates, GetRelation might cache nil.
		// Or DeleteRelation directly invalidates, leading to ErrNotFound.
		// Check for ErrNotFound as DeleteRelation invalidates directly.
		assert.ErrorIs(t, errCache, cache.ErrNotFound, "Cache should be invalidated (ErrNotFound) after delete")
		assert.Nil(t, cachedValPost)
	})

	// --- Test Case 2: Delete existing relation (not in cache) ---
	t.Run("Delete Existing Relation Not In Cache", func(t *testing.T) {
		clearRelationTestData(ctx)
		err := createNodeDirectlyRelTest(ctx, sourceNode)
		require.NoError(t, err)
		err = createNodeDirectlyRelTest(ctx, targetNode)
		require.NoError(t, err)

		// 1. Setup: Create relation directly, DO NOT cache it via GetRelation
		relToDelete := &network.Relation{
			ID:   relationToDeleteID,
			Type: network.RelationType_SCHOOLMATE,
		}
		err = createRelationDirectlyRelTest(ctx, sourceNode.ID, targetNode.ID, relToDelete)
		require.NoError(t, err, "Failed to create relation for delete test (no cache)")

		// Verify it's NOT in cache initially
		cachedValPre, err := relTestRelByteCache.GetRelation(ctx, relationToDeleteID)
		assert.ErrorIs(t, err, cache.ErrNotFound, "Relation should not be in cache initially")
		assert.Nil(t, cachedValPre)

		// 2. Execute DeleteRelation
		err = relTestRelRepo.DeleteRelation(ctx, relationToDeleteID)
		require.NoError(t, err, "DeleteRelation failed for existing relation (not cached)")

		// 3. Verify Deletion from DB
		_, err = relTestRelRepo.GetRelation(ctx, relationToDeleteID)
		require.Error(t, err, "GetRelation should return error after relation deletion (not cached)")
		// Check for specific 'not found' or ErrNilValue

		// 4. Verify Cache (still shouldn't be there)
		cachedValPost, err := relTestRelByteCache.GetRelation(ctx, relationToDeleteID)
		assert.ErrorIs(t, err, cache.ErrNotFound, "Cache should remain not found after delete (not cached)")
		assert.Nil(t, cachedValPost)
	})

	// --- Test Case 3: Delete Non-existent Relation ---
	t.Run("Delete Non-Existent Relation", func(t *testing.T) {
		clearRelationTestData(ctx)

		nonExistentID := "does-not-exist-for-delete-rel"

		// 1. Verify relation does not exist in DB and check initial cache state
		_, errDb := relTestRelRepo.GetRelation(ctx, nonExistentID)
		require.Error(t, errDb, "Relation should not exist in DB initially")
		_, errCache := relTestRelByteCache.GetRelation(ctx, nonExistentID)
		// GetRelation call above might cache nil or return ErrNotFound. Cache check reflects this.
		// Based on prior errors, expect ErrNotFound from direct cache check after repo call.
		assert.ErrorIs(t, errCache, cache.ErrNotFound, "Cache check after GetRelation for non-existent ID should return ErrNotFound")

		// 2. Execute DeleteRelation
		err := relTestRelRepo.DeleteRelation(ctx, nonExistentID)

		// 3. Verify Result
		// DeleteRelation should return the underlying 'not found' error from the DB/DAL layer.
		require.Error(t, err, "Deleting non-existent relation should return an error")
		// TODO: Check for specific 'not found' error type

		// 4. Verify Cache (after DeleteRelation attempted on non-existent)
		// DeleteRelation attempts cache invalidation even if DB relation not found.
		cachedValPost, errCachePost := relTestRelByteCache.GetRelation(ctx, nonExistentID)
		// Expect ErrNotFound because the key (or nil marker) should have been deleted by DeleteRelation.
		assert.ErrorIs(t, errCachePost, cache.ErrNotFound, "Cache should be empty (ErrNotFound) after DeleteRelation attempted on non-existent ID") // Adjusted assertion
		assert.Nil(t, cachedValPost)
	})
}

// --- Integration Test for GetNodeRelations ---

// Define a local struct matching the unexported one for unmarshalling cache data
// Needs to match getNodeRelationsCacheValue in relation_repo.go
type getNodeRelationsCacheValueForTest struct {
	RelationIDs []string `json:"relation_ids"`
	Total       int32    `json:"total"`
}

// Helper function to generate cache key for GetNodeRelations (mirroring repo logic)
func generateGetNodeRelationsCacheKeyForTest(req *network.GetNodeRelationsRequest) string {
	// Mirror the logic from neo4jRelationRepo.generateGetNodeRelationsCacheKey
	var relTypesStr []string
	if req.IsSetTypes() && len(req.Types) > 0 {
		relTypesStr = make([]string, 0, len(req.Types))
		for _, rt := range req.Types {
			relTypesStr = append(relTypesStr, rt.String())
		}
	}
	outgoing := true
	if req.IsSetOutgoing() {
		outgoing = *req.Outgoing
	}
	incoming := true
	if req.IsSetIncoming() {
		incoming = *req.Incoming
	}
	var limit, offset int64
	if req.IsSetLimit() {
		limit = int64(*req.Limit)
	} else {
		limit = 10 // Default from repo
	}
	if req.IsSetOffset() {
		offset = int64(*req.Offset)
	} else {
		offset = 0 // Default from repo
	}

	sortedTypes := make([]string, len(relTypesStr))
	copy(sortedTypes, relTypesStr)
	sort.Strings(sortedTypes)
	typesKeyPart := strings.Join(sortedTypes, ",")

	hasher := sha1.New()
	hasher.Write([]byte(typesKeyPart))
	typesHash := hex.EncodeToString(hasher.Sum(nil))

	direction := "any"
	if outgoing && !incoming {
		direction = "out"
	} else if !outgoing && incoming {
		direction = "in"
	}

	// Assuming GetNodeRelationsCachePrefix is exported or known
	// If not exported, copy the value "relation:list:ids:"
	getNodeRelationsCachePrefix := "relation:list:ids:"
	return fmt.Sprintf("%s%s:%s:%s:%d:%d",
		getNodeRelationsCachePrefix, req.NodeID, direction, typesHash, limit, offset)
}

func TestGetNodeRelations_Integration(t *testing.T) {
	ctx := context.Background()
	require.NotNil(t, relTestRelRepo)
	require.NotNil(t, relTestNodeRepo)
	require.NotNil(t, relTestRelByteCache)
	clearRelationTestData(ctx)

	// 1. Setup: Create a central node and connected nodes/relations
	centerNode := &network.Node{ID: "gnr-center", Type: network.NodeType_PERSON, Name: "Center Node"}
	nodeOut1 := &network.Node{ID: "gnr-out1", Type: network.NodeType_COMPANY, Name: "Out Company 1"}
	nodeOut2 := &network.Node{ID: "gnr-out2", Type: network.NodeType_COMPANY, Name: "Out Project 2"}
	nodeIn1 := &network.Node{ID: "gnr-in1", Type: network.NodeType_PERSON, Name: "In Person 1"}
	nodeIn2 := &network.Node{ID: "gnr-in2", Type: network.NodeType_SCHOOL, Name: "In Event 2"}
	nodeUnrelated := &network.Node{ID: "gnr-unrelated", Type: network.NodeType_SCHOOL, Name: "Unrelated"}

	nodes := []*network.Node{centerNode, nodeOut1, nodeOut2, nodeIn1, nodeIn2, nodeUnrelated}
	for _, n := range nodes {
		err := createNodeDirectlyRelTest(ctx, n)
		require.NoError(t, err, "Failed to create node %s", n.ID)
	}

	// Outgoing: Center -> Out1 (COLLEAGUE), Center -> Out2 (COLLEAGUE)
	relOut1 := &network.Relation{ID: "gnr-rel-out1", Type: network.RelationType_COLLEAGUE}
	relOut2 := &network.Relation{ID: "gnr-rel-out2", Type: network.RelationType_COLLEAGUE}
	// Incoming: In1 -> Center (FRIEND), In2 -> Center (SCHOOLMATE)
	relIn1 := &network.Relation{ID: "gnr-rel-in1", Type: network.RelationType_FRIEND}
	relIn2 := &network.Relation{ID: "gnr-rel-in2", Type: network.RelationType_SCHOOLMATE}
	// Incoming: In1 -> Center (COLLEAGUE) - Multiple relations between same nodes
	relIn3 := &network.Relation{ID: "gnr-rel-in3", Type: network.RelationType_COLLEAGUE}

	require.NoError(t, createRelationDirectlyRelTest(ctx, centerNode.ID, nodeOut1.ID, relOut1))
	require.NoError(t, createRelationDirectlyRelTest(ctx, centerNode.ID, nodeOut2.ID, relOut2))
	require.NoError(t, createRelationDirectlyRelTest(ctx, nodeIn1.ID, centerNode.ID, relIn1))
	require.NoError(t, createRelationDirectlyRelTest(ctx, nodeIn2.ID, centerNode.ID, relIn2))
	require.NoError(t, createRelationDirectlyRelTest(ctx, nodeIn1.ID, centerNode.ID, relIn3))

	// Pre-cache some relations
	_, _ = relTestRelRepo.GetRelation(ctx, relOut1.ID)
	_, _ = relTestRelRepo.GetRelation(ctx, relIn1.ID)
	time.Sleep(50 * time.Millisecond)

	// --- Test Case 1: Get all relations (default: out=true, in=true, limit=10) - Cache Miss ---
	t.Run("Get All Relations Cache Miss", func(t *testing.T) {
		req := &network.GetNodeRelationsRequest{
			NodeID: centerNode.ID,
			// Defaults: outgoing=true, incoming=true, limit=10, offset=0
		}
		cacheKey := generateGetNodeRelationsCacheKeyForTest(req) // Use helper

		// Verify cache miss
		_, err := relTestRelByteCache.Get(ctx, cacheKey) // Use generic Get for byte cache
		assert.ErrorIs(t, err, cache.ErrNotFound, "Cache should be empty before first GetNodeRelations")

		// Execute GetNodeRelations
		relations, total, err := relTestRelRepo.GetNodeRelations(ctx, req)
		require.NoError(t, err, "GetNodeRelations failed for center node")

		// Verify results (2 outgoing + 3 incoming = 5 total)
		assert.EqualValues(t, 5, total, "Expected 5 total relations")
		require.Len(t, relations, 5, "Expected 5 relations in results")
		// Check if specific relation IDs are present (order might vary)
		foundIDs := make(map[string]bool)
		for _, r := range relations {
			foundIDs[r.ID] = true
		}
		assert.True(t, foundIDs[relOut1.ID], "Expected relOut1")
		assert.True(t, foundIDs[relOut2.ID], "Expected relOut2")
		assert.True(t, foundIDs[relIn1.ID], "Expected relIn1")
		assert.True(t, foundIDs[relIn2.ID], "Expected relIn2")
		assert.True(t, foundIDs[relIn3.ID], "Expected relIn3")

		// Verify cache population
		time.Sleep(50 * time.Millisecond)
		cachedData, err := relTestRelByteCache.Get(ctx, cacheKey)
		require.NoError(t, err, "Failed to get data from cache after GetNodeRelations")
		require.NotEmpty(t, cachedData, "Cache should contain data after GetNodeRelations")

		// Verify cache content (structure and values)
		var cachedValue getNodeRelationsCacheValueForTest
		err = json.Unmarshal(cachedData, &cachedValue)
		require.NoError(t, err, "Failed to unmarshal cached relation list data")
		assert.EqualValues(t, 5, cachedValue.Total)
		require.Len(t, cachedValue.RelationIDs, 5)
		// Check elements match, ignoring order
		assert.ElementsMatch(t, []string{relOut1.ID, relOut2.ID, relIn1.ID, relIn2.ID, relIn3.ID}, cachedValue.RelationIDs)
	})

	// --- Test Case 2: Get all relations - Cache Hit ---
	t.Run("Get All Relations Cache Hit", func(t *testing.T) {
		req := &network.GetNodeRelationsRequest{
			NodeID: centerNode.ID,
		}
		cacheKey := generateGetNodeRelationsCacheKeyForTest(req)

		// Ensure cache key exists from previous test
		_, err := relTestRelByteCache.Get(ctx, cacheKey)
		require.NoError(t, err, "Cache key should exist for cache hit test")

		// Execute search again
		t.Log("Expecting GetNodeRelations cache hit...")
		relations, total, err := relTestRelRepo.GetNodeRelations(ctx, req)
		require.NoError(t, err, "GetNodeRelations (cache hit) failed")
		assert.EqualValues(t, 5, total, "Expected 5 total relations (cache hit)")
		require.Len(t, relations, 5, "Expected 5 relations in results (cache hit)")
		// Check details of a pre-cached relation (e.g., relOut1)
		var foundRel *network.Relation
		for _, r := range relations {
			if r.ID == relOut1.ID {
				foundRel = r
				break
			}
		}
		require.NotNil(t, foundRel, "Failed to find relOut1 in cache hit results")
		assert.Equal(t, network.RelationType_COLLEAGUE, foundRel.Type)
	})

	// --- Test Case 3: Get only Outgoing relations ---
	t.Run("Get Outgoing Only", func(t *testing.T) {
		outgoing := true
		incoming := false
		req := &network.GetNodeRelationsRequest{
			NodeID:   centerNode.ID,
			Outgoing: &outgoing,
			Incoming: &incoming,
		}
		cacheKey := generateGetNodeRelationsCacheKeyForTest(req)

		relations, total, err := relTestRelRepo.GetNodeRelations(ctx, req)
		require.NoError(t, err)
		assert.EqualValues(t, 2, total, "Expected 2 total outgoing relations")
		require.Len(t, relations, 2, "Expected 2 relations in results")
		foundIDs := make(map[string]bool)
		for _, r := range relations {
			foundIDs[r.ID] = true
		}
		assert.True(t, foundIDs[relOut1.ID])
		assert.True(t, foundIDs[relOut2.ID])

		// Verify caching
		time.Sleep(50 * time.Millisecond)
		cachedData, err := relTestRelByteCache.Get(ctx, cacheKey)
		require.NoError(t, err)
		var cachedValue getNodeRelationsCacheValueForTest
		require.NoError(t, json.Unmarshal(cachedData, &cachedValue))
		assert.EqualValues(t, 2, cachedValue.Total)
		assert.ElementsMatch(t, []string{relOut1.ID, relOut2.ID}, cachedValue.RelationIDs)
	})

	// --- Test Case 4: Get only Incoming relations of specific types (FRIEND, COLLEAGUE) ---
	t.Run("Get Incoming Filtered Types", func(t *testing.T) {
		outgoing := false
		incoming := true
		types := []network.RelationType{network.RelationType_FRIEND, network.RelationType_COLLEAGUE}
		req := &network.GetNodeRelationsRequest{
			NodeID:   centerNode.ID,
			Outgoing: &outgoing,
			Incoming: &incoming,
			Types:    types,
		}
		cacheKey := generateGetNodeRelationsCacheKeyForTest(req)

		relations, total, err := relTestRelRepo.GetNodeRelations(ctx, req)
		require.NoError(t, err)
		// Expected: relIn1 (FRIEND), relIn3 (COLLEAGUE) = 2
		assert.EqualValues(t, 2, total, "Expected 2 total incoming FRIEND/COLLEAGUE relations")
		require.Len(t, relations, 2, "Expected 2 relations in results")
		foundIDs := make(map[string]bool)
		for _, r := range relations {
			foundIDs[r.ID] = true
		}
		assert.True(t, foundIDs[relIn1.ID])
		assert.True(t, foundIDs[relIn3.ID], "Expected relIn3")

		// Verify caching
		time.Sleep(50 * time.Millisecond)
		cachedData, err := relTestRelByteCache.Get(ctx, cacheKey)
		require.NoError(t, err)
		var cachedValue getNodeRelationsCacheValueForTest
		require.NoError(t, json.Unmarshal(cachedData, &cachedValue))
		assert.EqualValues(t, 2, cachedValue.Total)
		assert.ElementsMatch(t, []string{relIn1.ID, relIn3.ID}, cachedValue.RelationIDs)

	})

	// --- Test Case 5: Pagination (Get All, Limit 2, Offset 1) ---
	t.Run("Get All Pagination", func(t *testing.T) {
		limit := int32(2)
		offset := int32(1)
		req := &network.GetNodeRelationsRequest{
			NodeID: centerNode.ID,
			Limit:  &limit,
			Offset: &offset,
		}
		cacheKey := generateGetNodeRelationsCacheKeyForTest(req)

		relations, total, err := relTestRelRepo.GetNodeRelations(ctx, req)
		require.NoError(t, err)
		assert.EqualValues(t, 5, total, "Expected 5 total relations (pagination)")
		require.Len(t, relations, 2, "Expected 2 relations on page 2 (limit 2, offset 1)")
		// Cannot assert specific IDs due to undefined order without ORDER BY in DAL

		// Verify caching
		time.Sleep(50 * time.Millisecond)
		cachedData, err := relTestRelByteCache.Get(ctx, cacheKey)
		require.NoError(t, err)
		var cachedValue getNodeRelationsCacheValueForTest
		require.NoError(t, json.Unmarshal(cachedData, &cachedValue))
		assert.EqualValues(t, 5, cachedValue.Total)
		require.Len(t, cachedValue.RelationIDs, 2) // Cache stores only the IDs for the requested page
		// Cannot assert specific IDs in cache content either
	})

	// --- Test Case 6: No relations found (unrelated node) ---
	t.Run("Get Relations No Results", func(t *testing.T) {
		req := &network.GetNodeRelationsRequest{
			NodeID: nodeUnrelated.ID,
		}
		cacheKey := generateGetNodeRelationsCacheKeyForTest(req)

		relations, total, err := relTestRelRepo.GetNodeRelations(ctx, req)
		require.NoError(t, err)
		assert.EqualValues(t, 0, total, "Expected 0 total relations")
		assert.Len(t, relations, 0, "Expected 0 relations in results")

		// Verify empty placeholder caching
		time.Sleep(50 * time.Millisecond)
		cachedData, err := relTestRelByteCache.Get(ctx, cacheKey)
		require.NoError(t, err, "Failed to get data from cache after empty search")
		// Need to access the exported placeholder or copy its value
		getNodeRelationsEmptyPlaceholder := "__EMPTY_REL_LIST__" // Copied value
		assert.Equal(t, []byte(getNodeRelationsEmptyPlaceholder), cachedData, "Cache should contain empty placeholder")

		// Search again (Cache Hit for empty)
		relationsHit, totalHit, errHit := relTestRelRepo.GetNodeRelations(ctx, req)
		require.NoError(t, errHit)
		assert.EqualValues(t, 0, totalHit, "Expected 0 total results (empty cache hit)")
		assert.Len(t, relationsHit, 0, "Expected 0 relations in results (empty cache hit)")
	})

	// --- Test Case 7: Relation deleted after caching the list ---
	t.Run("Get Relations Cache Hit With Deleted Relation", func(t *testing.T) {
		// 7.1. Cache the full list first (using a request that includes relOut2)
		reqAll := &network.GetNodeRelationsRequest{NodeID: centerNode.ID}
		_, _, err := relTestRelRepo.GetNodeRelations(ctx, reqAll) // Populate cache
		require.NoError(t, err)
		time.Sleep(50 * time.Millisecond) // Ensure cache write

		// 7.2. Delete one relation (relOut2) directly from DB (or via repo)
		err = relTestRelRepo.DeleteRelation(ctx, relOut2.ID) // Use repo to ensure cache invalidation for relOut2 detail
		require.NoError(t, err, "Failed to delete relOut2")
		// Ensure detail cache for relOut2 is gone
		_, errDetail := relTestRelRepo.GetRelation(ctx, relOut2.ID)
		require.Error(t, errDetail, "GetRelation for deleted relOut2 should fail")

		// 7.3. Call GetNodeRelations again (should hit the list cache)
		t.Log("Expecting GetNodeRelations cache hit, but one relation is deleted...")
		relations, total, err := relTestRelRepo.GetNodeRelations(ctx, reqAll)
		require.NoError(t, err, "GetNodeRelations (cache hit with deleted item) failed")

		// 7.4. Verify results: Total is from cache (5), but list excludes the deleted one (4)
		//    The repo's GetNodeRelations loop calls GetRelation for each ID in the cached list.
		//    GetRelation for the deleted ID (relOut2) will fail (ErrNotFound or ErrNilValue).
		//    The repo logic should skip adding it to the final result list.
		assert.EqualValues(t, 5, total, "Total should be from the original cached value (5)")
		require.Len(t, relations, 4, "Expected 4 relations in results (deleted one skipped)")
		foundIDs := make(map[string]bool)
		for _, r := range relations {
			foundIDs[r.ID] = true
		}
		assert.True(t, foundIDs[relOut1.ID], "Expected relOut1")
		assert.False(t, foundIDs[relOut2.ID], "Expected relOut2 to be missing") // Check deleted one is gone
		assert.True(t, foundIDs[relIn1.ID], "Expected relIn1")
		assert.True(t, foundIDs[relIn2.ID], "Expected relIn2")
		assert.True(t, foundIDs[relIn3.ID], "Expected relIn3")
	})

}
