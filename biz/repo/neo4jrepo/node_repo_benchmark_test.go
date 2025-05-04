package neo4jrepo_test

import (
	"context"
	"fmt"
	"math/rand"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	// NOTE: Ensure necessary imports from node_repo_test.go are implicitly available
	// or add them explicitly if running benchmarks in isolation requires it.
	network "labelwall/biz/model/relationship/network"
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

// Add other benchmarks for NodeRepository here if needed
// e.g., BenchmarkGetNode_CacheHit, BenchmarkGetNode_CacheMiss, BenchmarkSearchNodes...
