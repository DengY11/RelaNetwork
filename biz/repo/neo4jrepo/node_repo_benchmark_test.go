package neo4jrepo_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"

	// NOTE: Ensure necessary imports from node_repo_test.go are implicitly available
	// or add them explicitly if running benchmarks in isolation requires it.
	network "labelwall/biz/model/relationship/network"
	"labelwall/pkg/cache"
	//"labelwall/biz/repo/neo4jrepo" // Package being tested
)

const (
	numBenchmarkNodes   = 10000 // Number of nodes for benchmark graph
	avgBenchmarkRels    = 3     // Average number of relationships per node
	benchmarkProfession = "BenchmarkEngineer"
)

// setupLargeNetworkForBenchmark creates a large graph for benchmarking GetNetwork.
// Uses concurrency for relationship creation.
func setupLargeNetworkForBenchmark(ctx context.Context, b *testing.B, driver neo4j.DriverWithContext) string {
	b.StopTimer()
	defer b.StartTimer()

	session := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)

	// 1. Clear existing data (Sequential)
	_, err := session.Run(ctx, "MATCH ()-[r]->() DELETE r", nil)
	if err != nil {
		b.Logf("Benchmark setup: Warning - clearing relationships failed: %v", err)
	}
	_, err = session.Run(ctx, "MATCH (n) DELETE n", nil)
	if err != nil {
		b.Logf("Benchmark setup: Warning - clearing nodes failed: %v", err)
	}

	// 2. Create nodes in batches using UNWIND (Sequential)
	nodesData := make([]map[string]any, numBenchmarkNodes)
	var startingNodeID string
	for i := 0; i < numBenchmarkNodes; i++ {
		nodeID := "bench-node-" + strconv.Itoa(i)
		profession := "Other"
		if i%10 == 0 {
			profession = benchmarkProfession
			if startingNodeID == "" {
				startingNodeID = nodeID
			}
		}
		nodesData[i] = map[string]any{
			"id":         nodeID,
			"name":       "Bench Node " + strconv.Itoa(i),
			"profession": profession,
			"created_at": time.Now().UTC(),
			"updated_at": time.Now().UTC(),
		}
	}

	_, err = session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		_, errRun := tx.Run(ctx, `
			UNWIND $nodes AS nodeProps
			CREATE (n:PERSON)
			SET n = nodeProps
		`, map[string]any{"nodes": nodesData})
		if errRun != nil {
			return nil, fmt.Errorf("node creation query failed: %w", errRun)
		}
		return nil, nil
	})
	if err != nil {
		b.Fatalf("Benchmark setup failed during node creation: %v", err)
	}

	// 3. Create relationships data (Sequential)
	relsData := make([]map[string]any, 0, numBenchmarkNodes*avgBenchmarkRels)
	source := rand.NewSource(time.Now().UnixNano())
	r := rand.New(source)
	for i := 0; i < numBenchmarkNodes; i++ {
		sourceID := "bench-node-" + strconv.Itoa(i)
		numRels := r.Intn(avgBenchmarkRels * 2)
		for j := 0; j < numRels; j++ {
			targetIdx := r.Intn(numBenchmarkNodes)
			if targetIdx == i {
				continue
			}
			targetID := "bench-node-" + strconv.Itoa(targetIdx)
			// Keep track of type for potential future use, even if CREATE uses :RELATED
			relType := "FRIEND"
			if r.Intn(2) == 0 {
				relType = "COLLEAGUE"
			}
			relsData = append(relsData, map[string]any{
				"source": sourceID,
				"target": targetID,
				"type":   relType, // Store original type info if needed later
				"id":     fmt.Sprintf("bench-rel-%d-%d", i, j),
				"props": map[string]any{
					"created_at": time.Now().UTC(),
					"updated_at": time.Now().UTC(),
					"weight":     r.Float64(),
				},
			})
		}
	}

	// 4. Create relationships concurrently in batches
	concurrencyLevel := runtime.NumCPU() // Use number of CPU cores for concurrency
	if concurrencyLevel < 1 {
		concurrencyLevel = 1
	}
	if len(relsData) == 0 {
		b.Logf("No relationships to create.")

	} else {

		batchSize := (len(relsData) + concurrencyLevel - 1) / concurrencyLevel // Calculate batch size
		if batchSize < 1 {
			batchSize = 1
		}

		var wg sync.WaitGroup
		errChan := make(chan error, concurrencyLevel) // Channel to collect errors

		b.Logf("Starting concurrent relationship creation: %d workers, batch size ~%d", concurrencyLevel, batchSize)

		for i := 0; i < len(relsData); i += batchSize {
			wg.Add(1)
			end := i + batchSize
			if end > len(relsData) {
				end = len(relsData)
			}
			batch := relsData[i:end]

			go func(batchData []map[string]any) {
				defer wg.Done()
				// Each goroutine executes its batch in a separate transaction
				_, txErr := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
					_, errRun := tx.Run(ctx, `
                        UNWIND $batch AS relData
                        MATCH (a:PERSON {id: relData.source})
                        MATCH (b:PERSON {id: relData.target})
                        CREATE (a)-[r:RELATED]->(b) // Use CREATE with a fixed type :RELATED
                        SET r = relData.props       // Set properties after creation
                        RETURN count(r)
                    `, map[string]any{"batch": batchData})
					if errRun != nil {
						// Return error to ExecuteWrite, which will propagate it
						return nil, fmt.Errorf("concurrent rel creation query failed: %w", errRun)
					}
					return nil, nil
				})
				if txErr != nil {
					errChan <- txErr // Send error to the channel
				}
			}(batch) // Pass batch as argument to goroutine
		}

		wg.Wait()      // Wait for all goroutines to finish
		close(errChan) // Close channel once all goroutines are done

		// Check for errors from goroutines
		for txErr := range errChan {
			if txErr != nil {
				// Log the first error encountered and fail the benchmark
				b.Fatalf("Benchmark setup failed during concurrent relationship creation: %v", txErr)
			}
		}
		b.Logf("Concurrent relationship creation finished.")

	} // end else block for len(relsData) > 0

	b.Logf("Benchmark setup complete: %d nodes, %d relationships created.", numBenchmarkNodes, len(relsData))

	if startingNodeID == "" {
		startingNodeID = "bench-node-0"
		b.Logf("Warning: No node found with profession '%s'. Using '%s' as starting point criteria.", benchmarkProfession, startingNodeID)
	}
	return startingNodeID
}

// BenchmarkGetNetwork_LargeGraph benchmarks the GetNetwork method with a large graph.
func BenchmarkGetNetwork_LargeGraph(b *testing.B) {
	// Ensure testRepo and testDriver are initialized (from TestMain in node_repo_test.go)
	if testRepo == nil || testDriver == nil {
		b.Skip("Skipping benchmark, test environment not initialized (run tests in package mode: go test ./... -bench=.)")
		return
	}

	ctx := context.Background()

	// Setup the large graph once for the benchmark run
	_ = setupLargeNetworkForBenchmark(ctx, b, testDriver) // Starting node ID not directly needed for criteria below

	// --- Define the request for the benchmark ---
	// Query starting from nodes with the specific benchmark profession
	req := &network.GetNetworkRequest{
		StartNodeCriteria: map[string]string{"profession": benchmarkProfession},
		Depth:             3, // Reduce depth to a reasonable value (e.g., 3)
		// Optionally add filters:
		// RelationTypes: []network.RelationType{network.RelationType_COLLEAGUE},
		// NodeTypes: []network.NodeType{network.NodeType_PERSON},
	}
	b.Logf("Benchmarking GetNetwork with StartCriteria: %v, Depth: %d", req.StartNodeCriteria, req.Depth)

	b.ResetTimer() // Reset timer AFTER setup and request definition

	// Run the GetNetwork function b.N times
	for i := 0; i < b.N; i++ {
		// Execute the actual function being benchmarked
		nodes, relations, err := testRepo.GetNetwork(ctx, req)
		if err != nil {
			// Stop benchmark if a call fails during the run
			b.Fatalf("GetNetwork failed during benchmark run (iteration %d): %v", i, err)
		}
		// Add a check to prevent the compiler from optimizing away the call.
		// Using fmt.Sprintf is a common way to ensure variables are "used".
		_ = fmt.Sprintf("%d nodes, %d relations", len(nodes), len(relations))
	}

	b.StopTimer() // Stop timer after the loop

	// Optional: Report custom metrics if needed (e.g., average nodes/rels returned)
	// You would need to collect these values inside the loop and calculate average after.
}

// BenchmarkGetNetwork_Concurrent100 benchmarks 100 concurrent GetNetwork calls.
func BenchmarkGetNetwork_Concurrent1000(b *testing.B) {
	// Ensure testRepo and testDriver are initialized (from TestMain in node_repo_test.go)
	if testRepo == nil || testDriver == nil {
		b.Skip("Skipping benchmark, test environment not initialized (run tests in package mode: go test ./... -bench=.)")
		return
	}

	ctx := context.Background()

	// Setup the large graph once for the benchmark run
	// Use a smaller graph for concurrent tests initially to avoid overwhelming the system
	// Adjust numBenchmarkNodes if needed, maybe back to 1000?
	_ = setupLargeNetworkForBenchmark(ctx, b, testDriver)

	// --- Define the request (same for all concurrent calls) ---
	req := &network.GetNetworkRequest{
		StartNodeCriteria: map[string]string{"profession": benchmarkProfession},
		Depth:             3, // Keep depth reasonable for concurrent test
	}
	b.Logf("Benchmarking 1000 concurrent GetNetwork calls with StartCriteria: %v, Depth: %d", req.StartNodeCriteria, req.Depth)

	concurrencyLevel := 1000

	b.ResetTimer() // Reset timer AFTER setup

	// The outer loop (b.N) will likely only run once because the inner part is slow.
	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup // Import "sync"
		wg.Add(concurrencyLevel)

		// Launch goroutines
		for j := 0; j < concurrencyLevel; j++ {
			go func() {
				defer wg.Done()
				// Execute the actual function being benchmarked in each goroutine
				nodes, relations, err := testRepo.GetNetwork(ctx, req)
				if err != nil {
					// Use b.Error for non-fatal errors in goroutines
					// Using b.Fatal might stop the entire benchmark prematurely
					b.Errorf("Concurrent GetNetwork call failed: %v", err)
					return
				}
				// Add a check to prevent the compiler from optimizing away the call.
				_ = fmt.Sprintf("%d nodes, %d relations", len(nodes), len(relations))
			}()
		}

		// Wait for all goroutines launched in this iteration to complete
		wg.Wait()
	}

	b.StopTimer() // Stop timer after the loop
}

// BenchmarkCreateNode_LargeData benchmarks creating new nodes.
func BenchmarkCreateNode_LargeData(b *testing.B) {
	if testRepo == nil || testDriver == nil {
		b.Skip("Skipping benchmark, test environment not initialized")
		return
	}
	ctx := context.Background()

	// No specific setup needed, we just create new nodes each time.
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Generate unique-ish data for each creation
		name := "BenchCreateNode " + strconv.Itoa(i)
		prof := "Creator"
		req := &network.CreateNodeRequest{
			Type:       network.NodeType_PERSON,
			Name:       name,
			Profession: &prof,
			Properties: map[string]string{"iter": strconv.Itoa(i)},
		}

		createdNode, err := testRepo.CreateNode(ctx, req)
		if err != nil {
			b.Fatalf("CreateNode failed during benchmark (iteration %d): %v", i, err)
		}
		// Ensure the result is used
		_ = fmt.Sprintf("Created ID: %s", createdNode.ID)
	}
	b.StopTimer()
}

// BenchmarkGetNode_LargeData_Miss benchmarks getting nodes that are not in the cache.
func BenchmarkGetNode_LargeData_Miss(b *testing.B) {
	if testRepo == nil || testDriver == nil || testCache == nil {
		b.Skip("Skipping benchmark, test environment not initialized")
		return
	}
	ctx := context.Background()

	// Setup: Ensure data exists, but cache is clear for the target IDs.
	_ = setupLargeNetworkForBenchmark(ctx, b, testDriver)

	// Prepare IDs to fetch (assuming setup created bench-node-0 to bench-node-numBenchmarkNodes-1)
	numNodesToFetch := numBenchmarkNodes
	if numNodesToFetch == 0 {
		b.Skip("Skipping benchmark, setup did not create nodes.")
		return
	}
	nodeIDs := make([]string, numNodesToFetch)
	for i := 0; i < numNodesToFetch; i++ {
		nodeIDs[i] = "bench-node-" + strconv.Itoa(i)
	}

	// Ensure cache is clear before starting the timed portion
	b.StopTimer() // Stop timer for cache clearing
	clearCacheForIDs(ctx, b, testCache, nodeIDs, "node")
	b.StartTimer() // Restart timer for the actual benchmark loop
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Pick an ID to fetch - cycle through the list
		fetchID := nodeIDs[i%numNodesToFetch]

		// Get the node (expecting DB query + cache set)
		node, err := testRepo.GetNode(ctx, fetchID)
		if err != nil {
			// If setup guarantees IDs exist, this is an error
			b.Errorf("GetNode failed for existing ID %s (iteration %d): %v", fetchID, i, err)
		}
		if node == nil && err == nil {
			// Should not happen if ID exists
			b.Errorf("GetNode returned nil node without error for ID %s (iteration %d)", fetchID, i)
		}
		// Ensure the result is used
		if node != nil {
			_ = fmt.Sprintf("Fetched: %s", node.ID)
		}

		// Optimization: To truly measure *miss* performance without subsequent hits
		// interfering, we could clear the cache for the fetched ID *after* getting it,
		// but that adds overhead to the measured loop. Cycling IDs is a compromise.
	}
	b.StopTimer()
}

// BenchmarkGetNode_LargeData_Hit benchmarks getting nodes that are already in the cache.
func BenchmarkGetNode_LargeData_Hit(b *testing.B) {
	if testRepo == nil || testDriver == nil || testCache == nil {
		b.Skip("Skipping benchmark, test environment not initialized")
		return
	}
	ctx := context.Background()

	// Setup: Ensure data exists and cache is populated.
	_ = setupLargeNetworkForBenchmark(ctx, b, testDriver)

	numNodesToPreCache := 1000 // Pre-cache a subset for hit testing
	if numNodesToPreCache > numBenchmarkNodes {
		numNodesToPreCache = numBenchmarkNodes
	}
	if numNodesToPreCache == 0 {
		b.Skip("Skipping benchmark, setup did not create nodes.")
		return
	}

	preCachedIDs := make([]string, numNodesToPreCache)
	for i := 0; i < numNodesToPreCache; i++ {
		preCachedIDs[i] = "bench-node-" + strconv.Itoa(i)
	}

	// Pre-populate the cache
	b.Logf("Pre-caching %d nodes for hit benchmark...", numNodesToPreCache)
	b.StopTimer() // Stop timer for pre-caching
	for _, id := range preCachedIDs {
		_, err := testRepo.GetNode(ctx, id) // Call GetNode to populate cache
		if err != nil {
			b.Logf("Warning: Failed to pre-cache node %s: %v", id, err)
			// Decide if this should be fatal? For now, log and continue.
		}
	}
	// Optional: Add a small delay to ensure cache writes complete if async
	// time.Sleep(100 * time.Millisecond)
	b.StartTimer() // Restart timer for the actual benchmark loop
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Pick an ID known to be cached - cycle through the pre-cached list
		fetchID := preCachedIDs[i%numNodesToPreCache]

		// Get the node (expecting cache hit)
		node, err := testRepo.GetNode(ctx, fetchID)
		if err != nil {
			b.Errorf("GetNode failed for cached ID %s (iteration %d): %v", fetchID, i, err)
		}
		if node == nil && err == nil {
			b.Errorf("GetNode returned nil node without error for cached ID %s (iteration %d)", fetchID, i)
		}
		// Ensure the result is used
		if node != nil {
			_ = fmt.Sprintf("Fetched (hit): %s", node.ID)
		}
	}
	b.StopTimer()
}

// BenchmarkUpdateNode_LargeData benchmarks updating existing nodes.
func BenchmarkUpdateNode_LargeData(b *testing.B) {
	if testRepo == nil || testDriver == nil {
		b.Skip("Skipping benchmark, test environment not initialized")
		return
	}
	ctx := context.Background()

	// Setup: Ensure data exists.
	_ = setupLargeNetworkForBenchmark(ctx, b, testDriver)

	// Prepare IDs to update (assuming setup created bench-node-0 to bench-node-numBenchmarkNodes-1)
	numNodesToUpdate := numBenchmarkNodes
	if numNodesToUpdate == 0 {
		b.Skip("Skipping benchmark, setup did not create nodes.")
		return
	}
	nodeIDs := make([]string, numNodesToUpdate)
	for i := 0; i < numNodesToUpdate; i++ {
		nodeIDs[i] = "bench-node-" + strconv.Itoa(i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Pick an ID to update - cycle through the list
		updateID := nodeIDs[i%numNodesToUpdate]

		// Prepare update request
		newName := "UpdatedName " + strconv.Itoa(i)
		newStatus := "updated"
		req := &network.UpdateNodeRequest{
			ID:   updateID,
			Name: &newName,
			Properties: map[string]string{
				"status":    newStatus,
				"update_ts": strconv.FormatInt(time.Now().UnixNano(), 10),
			},
		}

		// Update the node (includes DB update + cache invalidation)
		updatedNode, err := testRepo.UpdateNode(ctx, req)
		if err != nil {
			// If setup guarantees IDs exist, this is an error
			b.Errorf("UpdateNode failed for existing ID %s (iteration %d): %v", updateID, i, err)
		}
		if updatedNode == nil && err == nil {
			// Should not happen if ID exists and update succeeds
			b.Errorf("UpdateNode returned nil node without error for ID %s (iteration %d)", updateID, i)
		}
		// Ensure the result is used
		if updatedNode != nil {
			_ = fmt.Sprintf("Updated: %s", updatedNode.ID)
		}
	}
	b.StopTimer()
}

// BenchmarkDeleteNode_LargeData benchmarks deleting existing nodes.
// This benchmark creates the node to be deleted within the loop to ensure it exists.
func BenchmarkDeleteNode_LargeData(b *testing.B) {
	if testRepo == nil || testDriver == nil {
		b.Skip("Skipping benchmark, test environment not initialized")
		return
	}
	ctx := context.Background()

	// Setup: Can optionally ensure the general large dataset exists for context,
	// but the nodes actually deleted are created within the loop.
	// _ = setupLargeNetworkForBenchmark(ctx, b, testDriver)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer() // Stop timer for setup within loop (CreateNode)

		// Create a unique node specifically for this deletion iteration
		deleteName := "BenchDeleteNode " + strconv.Itoa(i)
		prof := "Deleter"
		createReq := &network.CreateNodeRequest{
			Type:       network.NodeType_PERSON,
			Name:       deleteName,
			Profession: &prof,
			Properties: map[string]string{"iter": strconv.Itoa(i), "uuid": uuid.NewString()},
		}
		createdNode, errCreate := testRepo.CreateNode(ctx, createReq)
		if errCreate != nil {
			b.Fatalf("Failed to create node for delete benchmark (iteration %d): %v", i, errCreate)
		}
		deleteID := createdNode.ID // Use the actual created ID

		b.StartTimer() // Start timer ONLY for the DeleteNode call

		// Delete the node (includes DB delete + cache invalidation)
		err := testRepo.DeleteNode(ctx, deleteID)

		b.StopTimer() // Stop timer immediately after DeleteNode

		if err != nil {
			// We expect no error since we just created it.
			b.Errorf("DeleteNode failed during benchmark run for ID %s (iteration %d): %v", deleteID, i, err)
		}
		// No result to use for DeleteNode
	}
	// Final StopTimer is called implicitly by benchmark framework
}

// --- Concurrent Benchmarks ---

const defaultConcurrencyLevel = 10000 // Default number of goroutines for concurrent tests

// BenchmarkCreateNode_Concurrent benchmarks concurrent node creation.
func BenchmarkCreateNode_Concurrent(b *testing.B) {
	if testRepo == nil || testDriver == nil {
		b.Skip("Skipping benchmark, test environment not initialized")
		return
	}
	ctx := context.Background()
	concurrencyLevel := defaultConcurrencyLevel

	// No specific setup needed, each goroutine creates a new node.
	b.ResetTimer()

	for i := 0; i < b.N; i++ { // b.N loop usually runs once for concurrent tests
		var wg sync.WaitGroup
		wg.Add(concurrencyLevel)

		for j := 0; j < concurrencyLevel; j++ {
			go func(goRoutineID int) {
				defer wg.Done()

				// Generate unique data for each creation
				// Add GoRoutine ID and iteration to ensure uniqueness across restarts
				name := fmt.Sprintf("BenchConcCreate %d-%d", goRoutineID, time.Now().UnixNano())
				prof := "ConcurrentCreator"
				req := &network.CreateNodeRequest{
					Type:       network.NodeType_PERSON,
					Name:       name,
					Profession: &prof,
					Properties: map[string]string{"goroutine": strconv.Itoa(goRoutineID)},
				}

				createdNode, err := testRepo.CreateNode(ctx, req)
				if err != nil {
					b.Errorf("Concurrent CreateNode failed: %v", err)
					return
				}
				_ = fmt.Sprintf("Created ID: %s", createdNode.ID)
			}(j)
		}
		wg.Wait() // Wait for all concurrent creations in this batch
	}

	b.StopTimer()
}

// BenchmarkGetNode_Concurrent_Miss benchmarks concurrent cache misses.
func BenchmarkGetNode_Concurrent_Miss(b *testing.B) {
	if testRepo == nil || testDriver == nil || testCache == nil {
		b.Skip("Skipping benchmark, test environment not initialized")
		return
	}
	ctx := context.Background()
	concurrencyLevel := defaultConcurrencyLevel

	// Setup: Ensure data exists, cache is clear.
	_ = setupLargeNetworkForBenchmark(ctx, b, testDriver)
	numNodesToFetch := numBenchmarkNodes
	if numNodesToFetch < concurrencyLevel {
		numNodesToFetch = concurrencyLevel // Ensure enough unique IDs
		b.Logf("Warning: numBenchmarkNodes < concurrencyLevel, some IDs might be repeated slightly more often.")
	}
	if numNodesToFetch == 0 {
		b.Skip("Skipping benchmark, setup did not create nodes.")
		return
	}
	nodeIDs := make([]string, numNodesToFetch)
	for i := 0; i < numNodesToFetch; i++ {
		nodeIDs[i] = "bench-node-" + strconv.Itoa(i)
	}

	// Clear cache right before the timed section
	b.StopTimer()
	clearCacheForIDs(ctx, b, testCache, nodeIDs, "node")
	b.StartTimer()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		wg.Add(concurrencyLevel)

		for j := 0; j < concurrencyLevel; j++ {
			go func(goRoutineID int) {
				defer wg.Done()
				// Pick a unique ID for this goroutine in the batch
				fetchID := nodeIDs[goRoutineID%numNodesToFetch]

				node, err := testRepo.GetNode(ctx, fetchID)
				if err != nil && !errors.Is(err, cache.ErrNilValue) { // Allow ErrNilValue if DB was cleared but placeholder remains
					b.Errorf("Concurrent GetNode (miss) failed for ID %s: %v", fetchID, err)
				}
				if node == nil && err == nil {
					b.Errorf("Concurrent GetNode (miss) returned nil node without error for ID %s", fetchID)
				}
				if node != nil {
					_ = fmt.Sprintf("Fetched (miss): %s", node.ID)
				}

				// To ensure subsequent iterations are also misses, optionally clear the cache for this ID here.
				// This adds overhead but makes it a pure miss test.
				// _ = testCache.DeleteNode(ctx, fetchID)
			}(j)
		}
		wg.Wait()
	}

	b.StopTimer()
}

// BenchmarkGetNode_Concurrent_Hit benchmarks concurrent cache hits.
func BenchmarkGetNode_Concurrent_Hit(b *testing.B) {
	if testRepo == nil || testDriver == nil || testCache == nil {
		b.Skip("Skipping benchmark, test environment not initialized")
		return
	}
	ctx := context.Background()
	concurrencyLevel := defaultConcurrencyLevel

	// Setup: Ensure data exists and cache is populated.
	_ = setupLargeNetworkForBenchmark(ctx, b, testDriver)
	numNodesToPreCache := concurrencyLevel * 2 // Pre-cache more nodes than concurrency level
	if numNodesToPreCache > numBenchmarkNodes {
		numNodesToPreCache = numBenchmarkNodes
	}
	if numNodesToPreCache == 0 {
		b.Skip("Skipping benchmark, setup did not create nodes.")
		return
	}

	preCachedIDs := make([]string, numNodesToPreCache)
	for i := 0; i < numNodesToPreCache; i++ {
		preCachedIDs[i] = "bench-node-" + strconv.Itoa(i)
	}

	// Pre-populate the cache
	b.Logf("Pre-caching %d nodes for concurrent hit benchmark...", numNodesToPreCache)
	b.StopTimer() // Stop timer for pre-caching
	for _, id := range preCachedIDs {
		_, err := testRepo.GetNode(ctx, id)
		if err != nil {
			b.Logf("Warning: Failed to pre-cache node %s: %v", id, err)
		}
	}
	b.StartTimer()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		wg.Add(concurrencyLevel)

		for j := 0; j < concurrencyLevel; j++ {
			go func(goRoutineID int) {
				defer wg.Done()
				// Pick an ID known to be cached - cycle through the pre-cached list
				fetchID := preCachedIDs[goRoutineID%numNodesToPreCache]

				node, err := testRepo.GetNode(ctx, fetchID)
				if err != nil {
					b.Errorf("Concurrent GetNode (hit) failed for cached ID %s: %v", fetchID, err)
				}
				if node == nil && err == nil {
					b.Errorf("Concurrent GetNode (hit) returned nil node without error for cached ID %s", fetchID)
				}
				if node != nil {
					_ = fmt.Sprintf("Fetched (hit): %s", node.ID)
				}
			}(j)
		}
		wg.Wait()
	}

	b.StopTimer()
}

// BenchmarkUpdateNode_Concurrent benchmarks concurrent node updates.
func BenchmarkUpdateNode_Concurrent(b *testing.B) {
	if testRepo == nil || testDriver == nil {
		b.Skip("Skipping benchmark, test environment not initialized")
		return
	}
	ctx := context.Background()
	concurrencyLevel := defaultConcurrencyLevel

	// Setup: Ensure data exists.
	_ = setupLargeNetworkForBenchmark(ctx, b, testDriver)
	numNodesToUpdate := numBenchmarkNodes
	if numNodesToUpdate < concurrencyLevel {
		numNodesToUpdate = concurrencyLevel
	}
	if numNodesToUpdate == 0 {
		b.Skip("Skipping benchmark, setup did not create nodes.")
		return
	}
	nodeIDs := make([]string, numNodesToUpdate)
	for i := 0; i < numNodesToUpdate; i++ {
		nodeIDs[i] = "bench-node-" + strconv.Itoa(i)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		wg.Add(concurrencyLevel)

		for j := 0; j < concurrencyLevel; j++ {
			go func(goRoutineID int) {
				defer wg.Done()
				// Pick an ID to update - cycle through the list
				updateID := nodeIDs[goRoutineID%numNodesToUpdate]

				// Prepare update request with unique data
				newName := fmt.Sprintf("ConcUpdate %d-%d", goRoutineID, time.Now().UnixNano())
				newStatus := "concurrently-updated"
				req := &network.UpdateNodeRequest{
					ID:   updateID,
					Name: &newName,
					Properties: map[string]string{
						"status":      newStatus,
						"conc_update": strconv.Itoa(goRoutineID),
					},
				}

				updatedNode, err := testRepo.UpdateNode(ctx, req)
				if err != nil {
					b.Errorf("Concurrent UpdateNode failed for ID %s: %v", updateID, err)
				}
				if updatedNode == nil && err == nil {
					b.Errorf("Concurrent UpdateNode returned nil node without error for ID %s", updateID)
				}
				if updatedNode != nil {
					_ = fmt.Sprintf("Updated (conc): %s", updatedNode.ID)
				}
			}(j)
		}
		wg.Wait()
	}

	b.StopTimer()
}

// TODO: BenchmarkDeleteNode_Concurrent requires careful setup to pre-create deletable nodes.

// Helper function to clear cache for specific IDs (Node or Relation)
func clearCacheForIDs(ctx context.Context, b *testing.B, c cache.NodeAndByteCache, ids []string, keyType string) {
	// This is a basic implementation assuming NodeAndByteCache is the common interface
	// and we can infer the method based on keyType. A more robust solution might
	// require separate cache interfaces or type assertions.

	// testCache is likely *RedisCache, which implements both NodeCache and RelationCache
	// We need access to the specific Delete* methods.
	nodeCache, okNode := c.(cache.NodeCache)
	// relCache, okRel := c.(cache.RelationCache) // Assuming we might need this later

	if !okNode {
		b.Logf("Warning: Cache does not implement NodeCache, cannot clear node cache specifically.")
		// Fallback to generic delete if possible, or skip?
		// For now, just return if we can't get the specific interface.
		return
	}

	for _, id := range ids {
		var err error
		switch keyType {
		case "node":
			err = nodeCache.DeleteNode(ctx, id)
		// case "relation":
		// 	if okRel {
		// 		err = relCache.DeleteRelation(ctx, id)
		// 	} else {
		// 		b.Logf("Warning: Cache does not implement RelationCache, cannot clear relation %s", id)
		// 	}
		default:
			b.Logf("Warning: Unknown keyType '%s' for cache clearing.", keyType)
			continue
		}

		if err != nil && !errors.Is(err, cache.ErrNotFound) && !errors.Is(err, cache.ErrNilValue) {
			// Log non-critical errors during cache clear for benchmarks
			b.Logf("Warning: Failed to clear cache for %s ID %s: %v", keyType, id, err)
		}
	}
}

// Add other benchmarks for NodeRepository here if needed
// e.g., BenchmarkGetNode_CacheHit, BenchmarkGetNode_CacheMiss, BenchmarkSearchNodes...
