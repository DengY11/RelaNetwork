package neo4jrepo_test

import (
	"context"
	"fmt"
	"math/rand"
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
// Returns the ID of a node known to have the benchmarkProfession.
// IMPORTANT: This function assumes APOC procedures are available in Neo4j for efficient relationship creation.
func setupLargeNetworkForBenchmark(ctx context.Context, b *testing.B, driver neo4j.DriverWithContext) string {
	b.StopTimer()        // Stop timer during setup
	defer b.StartTimer() // Restart timer after setup

	session := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)

	// 1. Clear existing data
	// Use manual clear as clearTestData might have Redis dependencies not available here
	_, err := session.Run(ctx, "MATCH ()-[r]->() DELETE r", nil)
	if err != nil {
		b.Logf("Benchmark setup: Warning - clearing relationships failed: %v", err) // Log warning
	}
	_, err = session.Run(ctx, "MATCH (n) DELETE n", nil)
	if err != nil {
		b.Logf("Benchmark setup: Warning - clearing nodes failed: %v", err) // Log warning
	}

	// 2. Create nodes in batches using UNWIND
	nodesData := make([]map[string]any, numBenchmarkNodes)
	var startingNodeID string // Find a node with the target profession
	for i := 0; i < numBenchmarkNodes; i++ {
		nodeID := "bench-node-" + strconv.Itoa(i)
		profession := "Other"
		// Assign the benchmark profession to a fraction of nodes
		if i%10 == 0 {
			profession = benchmarkProfession
			if startingNodeID == "" { // Grab the first one as a potential starting point
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
		// Check error inside transaction work function
		if errRun != nil {
			return nil, fmt.Errorf("node creation query failed: %w", errRun)
		}
		return nil, nil
	})
	if err != nil {
		b.Fatalf("Benchmark setup failed during node creation: %v", err)
	}

	// 3. Create relationships in batches using UNWIND
	relsData := make([]map[string]any, 0, numBenchmarkNodes*avgBenchmarkRels)
	source := rand.NewSource(time.Now().UnixNano())
	r := rand.New(source)

	for i := 0; i < numBenchmarkNodes; i++ {
		sourceID := "bench-node-" + strconv.Itoa(i)
		numRels := r.Intn(avgBenchmarkRels * 2) // Random number of relations per node
		for j := 0; j < numRels; j++ {
			targetIdx := r.Intn(numBenchmarkNodes)
			if targetIdx == i { // Avoid self-loops
				continue
			}
			targetID := "bench-node-" + strconv.Itoa(targetIdx)
			relType := "FRIEND"
			if r.Intn(2) == 0 {
				relType = "COLLEAGUE"
			}
			relsData = append(relsData, map[string]any{
				"source": sourceID,
				"target": targetID,
				"type":   relType, // Storing type here for Cypher
				"id":     fmt.Sprintf("bench-rel-%d-%d", i, j),
				"props": map[string]any{ // Properties for the relationship itself
					"created_at": time.Now().UTC(),
					"updated_at": time.Now().UTC(),
					"weight":     r.Float64(),
				},
			})
		}
	}

	// Use relationship creation query relying on APOC
	_, err = session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		// Optimized query to create relationships between existing nodes using APOC
		/* // Comment out APOC call
				_, errRun := tx.Run(ctx, `
		            UNWIND $rels AS relData
		            MATCH (a:PERSON {id: relData.source})
		            MATCH (b:PERSON {id: relData.target})
		            CALL apoc.create.relationship(a, relData.type, relData.props, b) YIELD rel
		            RETURN count(rel)
		        `, map[string]any{"rels": relsData})
				// Check error inside transaction work function
				if errRun != nil {
					// Provide alternatives if APOC fails
					b.Logf(`Relationship creation with APOC failed: %v.
					Ensure APOC is installed or modify the benchmark setup query.
					See commented alternatives in node_repo_benchmark_test.go`, errRun)
					return nil, fmt.Errorf("relationship creation query failed (APOC needed): %w", errRun)
				}
		*/

		// ---- Alternatives without APOC (Commented out) ----
		// Alternative 1: Using MERGE (potentially slower)
		// Uncomment this section to use MERGE instead of APOC
		_, errRun := tx.Run(ctx, `
		    UNWIND $rels AS relData
		    MATCH (a:PERSON {id: relData.source})
		    MATCH (b:PERSON {id: relData.target})
		    // Cannot dynamically set type easily with standard MERGE/CREATE
		    // This creates only FRIEND relationships as an example. Modify if needed.
		    MERGE (a)-[r:FRIEND]->(b)
		    SET r = relData.props // Overwrites existing props if MERGE matched
		    RETURN count(r)
		`, map[string]any{"rels": relsData})
		if errRun != nil {
			return nil, fmt.Errorf("rel creation query failed (MERGE): %w", errRun)
		}

		return nil, nil
	})
	if err != nil {
		b.Fatalf("Benchmark setup failed during relationship creation: %v", err)
	}

	b.Logf("Benchmark setup complete: %d nodes, %d relationships created.", numBenchmarkNodes, len(relsData))

	if startingNodeID == "" {
		// If no node got the benchmark profession, pick the first node.
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
func BenchmarkGetNetwork_Concurrent10000(b *testing.B) {
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

	concurrencyLevel := 10000

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
