package tests

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/smogork/ISBD-MIMUW/pit"
	openapi1 "github.com/smogork/ISBD-MIMUW/pit/client/openapi1"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// COPY Operation Tests (Interface v1)
// ============================================================================

func TestV1_Copy(t *testing.T) {
	RequireInterfaceVersion(t, 1)

	dbClient := DbClientV1(BaseURL)
	ctx := context.Background()

	RunTracked(t, "Copy_Success_Headful", func(t *testing.T) {
		_ = SetupTestTableV1(t, dbClient, ctx, "people")

		// Execute COPY
		LoadTestDataV1(t, dbClient, ctx, "people")

		// Verify data was loaded
		result := ExecuteSelectStarV1(t, dbClient, ctx, "people")
		rows := ParseQueryResultsV1(result)
		require.Len(t, rows, 5, "Should have 5 rows after COPY")
	})

	// (removed unused helper)

	RunTracked(t, "Copy_Success_Mapping", func(t *testing.T) {
		_ = SetupTestTableV1(t, dbClient, ctx, "peoplemixed")
		_ = SetupTestTableV1(t, dbClient, ctx, "people")

		// Execute COPY
		LoadTestDataV1(t, dbClient, ctx, "people")
		LoadTestDataWithMappingV1(t, dbClient, ctx, "peoplemixed", []string{"id", "name", "surname", "age"})

		// Verify data was loaded
		resultNormal := ExecuteSelectStarV1(t, dbClient, ctx, "people")
		resultMixed := ExecuteSelectStarV1(t, dbClient, ctx, "peoplemixed")
		rowsNormal := ParseQueryResultsV1(resultNormal)
		rowsMixed := ParseQueryResultsV1(resultMixed)

		require.Len(t, rowsNormal, 5, "Should have 5 rows after COPY")
		require.Len(t, rowsMixed, 5, "Should have 5 rows after COPY")

		// Process results per-column across batches: for each column index, accumulate
		// sorted values across batches by merging sorted slices, then hash each column's
		// combined values. Finally compare the multiset of column-hashes between tables.

		// merge two sorted slices of strings into one sorted slice (used for column values)
		mergeSorted := func(a, b []string) []string {
			if len(a) == 0 {
				out := make([]string, len(b))
				copy(out, b)
				return out
			}
			if len(b) == 0 {
				out := make([]string, len(a))
				copy(out, a)
				return out
			}
			out := make([]string, 0, len(a)+len(b))
			i, j := 0, 0
			for i < len(a) && j < len(b) {
				if a[i] <= b[j] {
					out = append(out, a[i])
					i++
				} else {
					out = append(out, b[j])
					j++
				}
			}
			if i < len(a) {
				out = append(out, a[i:]...)
			}
			if j < len(b) {
				out = append(out, b[j:]...)
			}
			return out
		}

		// accumulate per-column values for a result (may be multi-batch)
		accumulateColumnValues := func(result []openapi1.QueryResultInner) [][]string {
			if len(result) == 0 {
				return nil
			}
			numCols := len(result[0].Columns)
			if numCols == 0 {
				return nil
			}

			accum := make([][]string, numCols)
			for _, batch := range result {
				// sanity: skip batches with different column count
				if len(batch.Columns) != numCols {
					continue
				}
				for colIdx := 0; colIdx < numCols; colIdx++ {
					valsI := extractColumnValuesV1(batch.Columns[colIdx])
					if len(valsI) == 0 {
						continue
					}
					vals := make([]string, 0, len(valsI))
					for _, v := range valsI {
						vals = append(vals, fmt.Sprint(v))
					}
					sort.Strings(vals)
					accum[colIdx] = mergeSorted(accum[colIdx], vals)
				}
			}
			return accum
		}

		accumColsNormal := accumulateColumnValues(resultNormal)
		accumColsMixed := accumulateColumnValues(resultMixed)

		// compute per-column hashes from accumulated (merged & sorted) values
		computeColHashes := func(accumCols [][]string) []string {
			if accumCols == nil {
				return nil
			}
			hashes := make([]string, 0, len(accumCols))
			for _, vals := range accumCols {
				combined := strings.Join(vals, "|")
				sum := sha256.Sum256([]byte(combined))
				hashes = append(hashes, hex.EncodeToString(sum[:]))
			}
			sort.Strings(hashes)
			return hashes
		}

		hashesNormal := computeColHashes(accumColsNormal)
		hashesMixed := computeColHashes(accumColsMixed)

		require.Equal(t, hashesNormal, hashesMixed, "Column contents should match regardless of column order and batching")
	})

	RunTracked(t, "Copy_Success_Headless", func(t *testing.T) {
		_ = SetupTestTableV1(t, dbClient, ctx, "people")

		// Execute COPY
		LoadTestDataHeadlessV1(t, dbClient, ctx, "people")

		// Verify data was loaded
		result := ExecuteSelectStarV1(t, dbClient, ctx, "people")
		rows := ParseQueryResultsV1(result)
		require.Len(t, rows, 5, "Should have 5 rows after COPY")
	})

	RunTracked(t, "Copy_ToNonExistentTable_Fails", func(t *testing.T) {
		t.Log(pit.FormatRequest("POST", "/query", map[string]interface{}{
			"sourceFilepath": "/data/tables/people/data.csv", "destinationTableName": "non_existent_table_xyz"}))
		queryId, resp, err := SubmitCopyQueryV1(dbClient, ctx,
			"/data/tables/people/data.csv",
			"non_existent_table_xyz",
			true)
		t.Log(pit.FormatResponse(resp))

		// May fail at submission (400) or during execution (FAILED)
		if err == nil && resp.StatusCode == http.StatusOK {
			query, waitErr := WaitForQueryCompletionV1(dbClient, ctx, queryId, 10*time.Second)
			require.NoError(t, waitErr)
			require.Equal(t, openapi1.FAILED, query.GetStatus(),
				"COPY to non-existent table should fail")
		} else {
			require.Equal(t, http.StatusBadRequest, resp.StatusCode,
				"COPY to non-existent table should return 400")
		}
	})

	RunTracked(t, "Copy_WithInvalidPath_Fails", func(t *testing.T) {
		_ = SetupTestTableV1(t, dbClient, ctx, "people")

		t.Log(pit.FormatRequest("POST", "/query", map[string]interface{}{
			"sourceFilepath": "/nonexistent/path/data.csv", "destinationTableName": "people"}))
		queryId, resp, err := SubmitCopyQueryV1(dbClient, ctx,
			"/nonexistent/path/data.csv",
			"people",
			true)
		t.Log(pit.FormatResponse(resp))

		if err == nil && resp.StatusCode == http.StatusOK {
			query, waitErr := WaitForQueryCompletionV1(dbClient, ctx, queryId, 10*time.Second)
			require.NoError(t, waitErr)
			require.Equal(t, openapi1.FAILED, query.GetStatus(),
				"COPY with invalid path should fail")
		}
	})

	RunTracked(t, "Copy_InvalidPath_NoPartialData", func(t *testing.T) {
		_ = SetupTestTableV1(t, dbClient, ctx, "people")

		// Try to COPY from invalid path
		t.Log(pit.FormatRequest("POST", "/query", map[string]interface{}{
			"sourceFilepath": "/nonexistent/path/data.csv", "destinationTableName": "people"}))
		queryId, resp, _ := SubmitCopyQueryV1(dbClient, ctx,
			"/nonexistent/path/data.csv",
			"people",
			true)
		t.Log(pit.FormatResponse(resp))

		if resp != nil && resp.StatusCode == http.StatusOK {
			WaitForQueryCompletionV1(dbClient, ctx, queryId, 10*time.Second)
		}

		// Table should remain empty (no partial data)
		result := ExecuteSelectStarV1(t, dbClient, ctx, "people")
		rows := ParseQueryResultsV1(result)
		require.Empty(t, rows, "Table should be empty after failed COPY")
	})

	RunTracked(t, "Copy_MultipleTimes_AccumulatesData", func(t *testing.T) {
		_ = SetupTestTableV1(t, dbClient, ctx, "types_test")

		// First COPY
		LoadTestDataHeadlessV1(t, dbClient, ctx, "types_test")
		result1 := ExecuteSelectStarV1(t, dbClient, ctx, "types_test")
		count1 := CountRowsV1(result1)
		require.Greater(t, count1, 0, "Should have rows after first COPY")

		// Second COPY - same data
		LoadTestDataHeadlessV1(t, dbClient, ctx, "types_test")
		result2 := ExecuteSelectStarV1(t, dbClient, ctx, "types_test")
		count2 := CountRowsV1(result2)

		require.Equal(t, count1*2, count2,
			"Second COPY should double the row count")
	})

	RunTracked(t, "Copy_WithHeader_ParsesCorrectly", func(t *testing.T) {
		_ = SetupTestTableV1(t, dbClient, ctx, "people")

		// COPY with header=true
		t.Log(pit.FormatRequest("POST", "/query", map[string]interface{}{
			"sourceFilepath": "/data/tables/people/data.csv", "destinationTableName": "people", "doesCsvContainHeader": true}))
		queryId, resp, err := SubmitCopyQueryV1(dbClient, ctx,
			"/data/tables/people/data.csv",
			"people",
			true)
		t.Log(pit.FormatResponse(resp))
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode)

		query, err := WaitForQueryCompletionV1(dbClient, ctx, queryId, 30*time.Second)
		require.NoError(t, err)
		require.Equal(t, openapi1.COMPLETED, query.GetStatus())

		// Verify correct number of rows (header not counted as data)
		result := ExecuteSelectStarV1(t, dbClient, ctx, "people")
		rows := ParseQueryResultsV1(result)
		require.Len(t, rows, 5, "Should have 5 data rows (header excluded)")
	})

	RunTracked(t, "Copy_EmptyTable_SelectReturnsEmpty", func(t *testing.T) {
		_ = SetupTestTableV1(t, dbClient, ctx, "people")
		// No COPY executed

		result := ExecuteSelectStarV1(t, dbClient, ctx, "people")
		rows := ParseQueryResultsV1(result)
		require.Empty(t, rows, "Empty table should return no rows")
	})
}

// ============================================================================
// COPY Atomicity Tests (Interface v1)
// ============================================================================

func TestV1_Copy_Atomicity(t *testing.T) {
	RequireInterfaceVersion(t, 1)

	dbClient := DbClientV1(BaseURL)
	ctx := context.Background()

	RunTracked(t, "Copy_FailureDoesNotAffectExistingData", func(t *testing.T) {
		_ = SetupTestTableV1(t, dbClient, ctx, "types_test")

		// First, load some valid data
		LoadTestDataV1(t, dbClient, ctx, "types_test")
		result1 := ExecuteSelectStarV1(t, dbClient, ctx, "types_test")
		count1 := CountRowsV1(result1)
		require.Greater(t, count1, 0, "Should have data after first COPY")

		// Try to COPY from invalid path
		t.Log(pit.FormatRequest("POST", "/query", map[string]interface{}{
			"sourceFilepath": "/nonexistent/invalid/path.csv", "destinationTableName": "types_test"}))
		queryId, resp, _ := SubmitCopyQueryV1(dbClient, ctx,
			"/nonexistent/invalid/path.csv",
			"types_test",
			true)
		t.Log(pit.FormatResponse(resp))

		if resp != nil && resp.StatusCode == http.StatusOK {
			WaitForQueryCompletionV1(dbClient, ctx, queryId, 10*time.Second)
		}

		// Original data should still be there
		result2 := ExecuteSelectStarV1(t, dbClient, ctx, "types_test")
		count2 := CountRowsV1(result2)
		require.Equal(t, count1, count2,
			"Failed COPY should not affect existing data")
	})

	RunTracked(t, "Copy_QueryStatusTransitions", func(t *testing.T) {
		_ = SetupTestTableV1(t, dbClient, ctx, "people")

		t.Log(pit.FormatRequest("POST", "/query", map[string]interface{}{
			"sourceFilepath": "/data/tables/people/data.csv", "destinationTableName": "people", "doesCsvContainHeader": true}))
		queryId, resp, err := SubmitCopyQueryV1(dbClient, ctx,
			"/data/tables/people/data.csv",
			"people",
			true)
		t.Log(pit.FormatResponse(resp))
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode)

		// Query should eventually complete
		query, err := WaitForQueryCompletionV1(dbClient, ctx, queryId, 30*time.Second)
		require.NoError(t, err)
		require.Equal(t, openapi1.COMPLETED, query.GetStatus())
	})
}
