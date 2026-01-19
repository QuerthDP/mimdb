package tests

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/smogork/ISBD-MIMUW/pit"
	openapi1 "github.com/smogork/ISBD-MIMUW/pit/client/openapi1"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// Stress Tests for Interface v1 - COPY Atomicity
// ============================================================================

// TestV1_Stress_CopyAtomicity tests that COPY operation is atomic.
// While COPY is in progress, concurrent SELECT queries should see either:
// - 0 rows (before COPY commits)
// - All rows (after COPY commits)
// Never partial data.
// Runs 100 iterations to maximize chance of catching race conditions.
func TestV1_Stress_CopyAtomicity(t *testing.T) {
	RequireInterfaceVersion(t, 1)

	if DbMemoryBytes <= 0 {
		t.Skip("Skipping stress test: -db-memory flag not set")
	}

	const iterations = 100

	RunTracked(t, "CopyAtomicity_100iterations", func(t *testing.T) {
		dbClient := DbClientV1(BaseURL)
		ctx := context.Background()

		// Track totals across all iterations
		totalSelects := 0
		totalPartial := 0

		for iter := 0; iter < iterations; iter++ {
			// Create fresh table for each iteration
			tableId := CreateTableV1(t, dbClient, ctx, mustReadSchemaV1(t, "stress_rows"))

			// Run single atomicity test
			selects, partial := runCopyAtomicityTest(t, dbClient, ctx, "stress_rows", iter+1, iterations)
			totalSelects += selects
			totalPartial += partial

			// Delete table to start fresh
			dbClient.SchemaAPI.DeleteTable(ctx, tableId).Execute()

			// Fail fast if we detected partial data
			if partial > 0 {
				t.Fatalf("Iteration %d/%d: PARTIAL DATA DETECTED! Stopping test.", iter+1, iterations)
			}
		}

		t.Logf("=== FINAL RESULTS ===")
		t.Logf("Total iterations: %d", iterations)
		t.Logf("Total SELECT queries: %d", totalSelects)
		t.Logf("Total partial observations: %d", totalPartial)

		require.Equal(t, 0, totalPartial, "No partial data should be observed across all iterations")
	})
}

// mustReadSchemaV1 reads schema or fails the test
func mustReadSchemaV1(t *testing.T, tableName string) *openapi1.TableSchema {
	schema, err := ReadTableSchemaV1(tableName)
	require.NoError(t, err)
	return schema
}

// runCopyAtomicityTest runs a single iteration of the atomicity test
// Returns (number of selects, number of partial observations)
func runCopyAtomicityTest(t *testing.T, dbClient *openapi1.APIClient, ctx context.Context, tableName string, iteration, total int) (int, int) {
	// Synchronization channels
	goroutineReady := make(chan struct{})
	copyDone := make(chan struct{})

	// Collect submitted query IDs
	var queryIds []string
	var queryIdsMutex sync.Mutex

	// Start SELECT goroutine
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		close(goroutineReady)

		for {
			select {
			case <-copyDone:
				if queryId := submitSelectV1(dbClient, ctx, tableName); queryId != "" {
					queryIdsMutex.Lock()
					queryIds = append(queryIds, queryId)
					queryIdsMutex.Unlock()
				}
				return
			default:
				if queryId := submitSelectV1(dbClient, ctx, tableName); queryId != "" {
					queryIdsMutex.Lock()
					queryIds = append(queryIds, queryId)
					queryIdsMutex.Unlock()
				}
				time.Sleep(5 * time.Millisecond)
			}
		}
	}()

	<-goroutineReady

	// Execute COPY
	LoadTestDataHeadlessV1(t, dbClient, ctx, tableName)

	close(copyDone)
	wg.Wait()

	// Collect results
	partialCount := 0
	successCount := 0

	for _, queryId := range queryIds {
		query, err := WaitForQueryCompletionV1(dbClient, ctx, queryId, 60*time.Second)
		if err != nil || query.GetStatus() != openapi1.COMPLETED {
			continue
		}

		result, err := GetQueryResultV1(dbClient, ctx, queryId)
		if err != nil {
			continue
		}

		rowCount := CountRowsV1(result)
		successCount++

		if rowCount != 0 && rowCount != stressTableRows {
			partialCount++
			t.Logf("Iteration %d/%d: PARTIAL DATA! Query returned %d rows (expected 0 or %d)",
				iteration, total, rowCount, stressTableRows)
		}
	}

	t.Logf("Iteration %d/%d: %d SELECTs, %d successful, %d partial",
		iteration, total, len(queryIds), successCount, partialCount)

	return len(queryIds), partialCount
}

// submitSelectV1 submits a SELECT * query and returns query ID (without waiting for result)
func submitSelectV1(apiClient *openapi1.APIClient, ctx context.Context, tableName string) string {
	selectQuery := &openapi1.SelectQuery{}
	selectQuery.SetTableName(tableName)

	queryDef := openapi1.SelectQueryAsQueryQueryDefinition(selectQuery)
	req := openapi1.ExecuteQueryRequest{QueryDefinition: queryDef}

	queryId, resp, err := apiClient.ExecutionAPI.SubmitQuery(ctx).ExecuteQueryRequest(req).Execute()
	if err != nil || resp.StatusCode != http.StatusOK {
		return "" // Query submission failed
	}

	return queryId
}

// ============================================================================
// Stress Tests for Interface v1 - Multiple Concurrent COPYs
// ============================================================================

// TestV1_Stress_ConcurrentCopy tests behavior when multiple COPY operations
// are attempted on the same table concurrently.
func TestV1_Stress_ConcurrentCopy(t *testing.T) {
	RequireInterfaceVersion(t, 1)

	if DbMemoryBytes <= 0 {
		t.Skip("Skipping stress test: -db-memory flag not set")
	}

	RunTracked(t, "ConcurrentCopy", func(t *testing.T) {
		dbClient := DbClientV1(BaseURL)
		ctx := context.Background()

		// Setup table
		_ = SetupTestTableV1(t, dbClient, ctx, "stress_rows")

		// Start multiple COPY operations concurrently
		const numCopies = 3
		var wg sync.WaitGroup
		results := make([]bool, numCopies) // true = success, false = failure

		for i := 0; i < numCopies; i++ {
			i := i
			wg.Add(1)
			go func() {
				defer wg.Done()
				results[i] = doCopyV1(t, dbClient, ctx, "stress_rows")
			}()
		}

		wg.Wait()

		// Count successes
		successCount := 0
		for _, success := range results {
			if success {
				successCount++
			}
		}

		t.Logf("Concurrent COPY results: %d/%d succeeded", successCount, numCopies)

		// Verify final state - should have consistent number of rows
		result := ExecuteSelectStarV1(t, dbClient, ctx, "stress_rows")
		rowCount := CountRowsV1(result)

		// Row count should be a multiple of stressTableRows (each successful COPY adds 8192 rows)

		require.Equal(t, numCopies*stressTableRows, rowCount,
			"Row count should be a multiple of %d (got %d)", stressTableRows, rowCount)

		t.Logf("Final row count: %d (expected multiple of %d)", rowCount, stressTableRows)
	})
}

// doCopyV1 executes a COPY operation and returns true if successful
func doCopyV1(t *testing.T, apiClient *openapi1.APIClient, ctx context.Context, tableName string) bool {
	dataPath := "/data/tables/stress_rows/data-headless.csv"

	copyQuery := &openapi1.CopyQuery{
		SourceFilepath:       dataPath,
		DestinationTableName: tableName,
	}
	doesContainHeader := false
	copyQuery.DoesCsvContainHeader = &doesContainHeader

	queryDef := openapi1.CopyQueryAsQueryQueryDefinition(copyQuery)
	req := openapi1.ExecuteQueryRequest{QueryDefinition: queryDef}

	t.Log(pit.FormatRequest("POST", "/query", copyQuery))
	queryId, resp, err := apiClient.ExecutionAPI.SubmitQuery(ctx).ExecuteQueryRequest(req).Execute()
	t.Log(pit.FormatResponse(resp))

	if err != nil || resp.StatusCode != http.StatusOK {
		return false
	}

	// Wait for completion
	query, err := WaitForQueryCompletionV1(apiClient, ctx, queryId, 60*time.Second)
	if err != nil {
		return false
	}

	return query.GetStatus() == openapi1.COMPLETED
}
