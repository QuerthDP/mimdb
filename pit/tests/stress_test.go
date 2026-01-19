package tests

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/smogork/ISBD-MIMUW/pit"
	apiclient "github.com/smogork/ISBD-MIMUW/pit/client/openapi2"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// Stress Test Configuration
// ============================================================================

const (
	// Number of rows in the stress_rows table
	stressTableRows = 8192

	// Base string length for REPLACE operations (power of 2 for clean calculations)
	baseStringLength = 128
)

// Base string used for REPLACE operations (~100 characters of 'a')
var baseString = "a"

// ============================================================================
// Helper Functions - Exponential REPLACE
// ============================================================================

func buildExponentialReplace(depth int) string {
	if depth <= 1 {
		return fmt.Sprintf("'%s'", baseString)
	}

	subExpr := buildExponentialReplace(depth - 1)
	return fmt.Sprintf("REPLACE(%s, 'a', 'aa')", subExpr)
}

// calculateExponentialDepth calculates the exact depth (as float) needed for targetBytes
// Total data = stressTableRows * baseStringLength * 2^(depth-1)
// Returns the precise depth value - caller decides to use Floor or Ceil
func calculateExponentialDepth(targetBytes int64) float64 {
	// targetBytes = stressTableRows * baseStringLength * 2^(depth-1)
	// 2^(depth-1) = targetBytes / (stressTableRows * baseStringLength)
	// depth-1 = log2(targetBytes / (stressTableRows * baseStringLength))
	// depth = 1 + log2(targetBytes / (stressTableRows * baseStringLength))

	bytesAtDepth1 := float64(stressTableRows * baseStringLength)
	ratio := float64(targetBytes) / bytesAtDepth1
	if ratio <= 1 {
		return 1.0
	}
	return 1.0 + math.Log2(ratio)
}

// estimateExponentialDataSize estimates total data size for exponential concat
func estimateExponentialDataSize(depth int) int64 {
	if depth < 1 {
		depth = 1
	}
	perRow := int64(baseStringLength) << (depth - 1) // baseStringLength * 2^(depth-1)
	return int64(stressTableRows) * perRow
}

// runStressQuery submits a query, waits for completion and verifies success
// Note: WaitForQueryCompletion automatically flushes the result
func runStressQuery(t *testing.T, dbClient *apiclient.APIClient, ctx context.Context, sql string, timeout time.Duration) {
	queryId, resp, err := SubmitSelectQuery(dbClient, ctx, sql)
	require.NoError(t, err, "Query submission should succeed")
	require.Equal(t, http.StatusOK, resp.StatusCode)

	t.Logf("Query submitted: %s", queryId)

	// Wait for completion (automatically flushes result)
	query, err := WaitForQueryCompletion(dbClient, ctx, queryId, timeout)
	require.NoError(t, err, "Query should complete within %v", timeout)

	if query.GetStatus() == apiclient.FAILED {
		problems, _ := GetQueryError(dbClient, ctx, queryId)
		t.Fatalf("Query failed: %+v", problems)
	}

	require.Equal(t, apiclient.COMPLETED, query.GetStatus(), "Query should complete successfully")
	t.Log("Query completed successfully")
}

// ============================================================================
// Stress Tests - Sorting
// ============================================================================

func TestStress_SortLargeData(t *testing.T) {
	RequireInterfaceVersion(t, 2)
	if DbMemoryBytes <= 0 {
		t.Skip("Skipping stress test: -db-memory flag not set")
	}

	dbClient := pit.DbClient(BaseURL)
	ctx := context.Background()

	// Setup table
	_ = SetupTestTable(t, dbClient, ctx, "stress_rows")
	LoadTestData(t, dbClient, ctx, "stress_rows")

	// Calculate depth to generate 2x memory (use Ceil to ensure we exceed)
	targetBytes := DbMemoryBytes * 2
	exactDepth := calculateExponentialDepth(targetBytes)
	depth := int(math.Ceil(exactDepth))
	estimatedSize := estimateExponentialDataSize(depth)

	t.Logf("Stress test configuration:")
	t.Logf("  Database memory: %d bytes (%.2f MB)", DbMemoryBytes, float64(DbMemoryBytes)/(1024*1024))
	t.Logf("  Target data size: %d bytes (%.2f MB)", targetBytes, float64(targetBytes)/(1024*1024))
	t.Logf("  Exact depth: %.2f, using Ceil: %d", exactDepth, depth)
	t.Logf("  Estimated data size: %d bytes (%.2f MB)", estimatedSize, float64(estimatedSize)/(1024*1024))
	t.Logf("  Rows: %d", stressTableRows)

	// Build the query with LIMIT 1 to minimize result storage
	replaceExpr := buildExponentialReplace(depth)
	sql := fmt.Sprintf("SELECT %s FROM stress_rows ORDER BY 0 ASC LIMIT 1", replaceExpr)

	t.Logf("Query length: %d chars", len(sql))

	runStressQuery(t, dbClient, ctx, sql, 5*time.Minute)
}

func TestStress_SortLargeData_Descending(t *testing.T) {
	RequireInterfaceVersion(t, 2)
	if DbMemoryBytes <= 0 {
		t.Skip("Skipping stress test: -db-memory flag not set")
	}

	dbClient := pit.DbClient(BaseURL)
	ctx := context.Background()

	_ = SetupTestTable(t, dbClient, ctx, "stress_rows")
	LoadTestData(t, dbClient, ctx, "stress_rows")

	targetBytes := DbMemoryBytes * 2
	exactDepth := calculateExponentialDepth(targetBytes)
	depth := int(math.Ceil(exactDepth))

	t.Logf("Testing DESC sort with depth %d (exact %.2f), estimated %.2f MB",
		depth, exactDepth, float64(estimateExponentialDataSize(depth))/(1024*1024))

	replaceExpr := buildExponentialReplace(depth)
	sql := fmt.Sprintf("SELECT %s FROM stress_rows ORDER BY 0 DESC LIMIT 1", replaceExpr)

	runStressQuery(t, dbClient, ctx, sql, 5*time.Minute)
}

func TestStress_SortMultipleColumns(t *testing.T) {
	RequireInterfaceVersion(t, 2)
	if DbMemoryBytes <= 0 {
		t.Skip("Skipping stress test: -db-memory flag not set")
	}

	dbClient := pit.DbClient(BaseURL)
	ctx := context.Background()

	_ = SetupTestTable(t, dbClient, ctx, "stress_rows")
	LoadTestData(t, dbClient, ctx, "stress_rows")

	// Use 1x memory per column (2x total)
	targetBytes := DbMemoryBytes
	exactDepth := calculateExponentialDepth(targetBytes)
	depth := int(math.Ceil(exactDepth))

	t.Logf("Testing multi-column sort, depth %d, estimated %.2f MB total",
		depth, float64(estimateExponentialDataSize(depth)*2)/(1024*1024))

	replaceExpr1 := buildExponentialReplace(depth)
	replaceExpr2 := buildExponentialReplace(depth)
	sql := fmt.Sprintf("SELECT %s, %s FROM stress_rows ORDER BY 0 ASC, 1 DESC LIMIT 1", replaceExpr1, replaceExpr2)

	runStressQuery(t, dbClient, ctx, sql, 10*time.Minute)
}

// ============================================================================
// Parameterized Stress Tests (different data sizes)
// ============================================================================

func TestStress_Incremental(t *testing.T) {
	RequireInterfaceVersion(t, 2)
	if DbMemoryBytes <= 0 {
		t.Skip("Skipping stress test: -db-memory flag not set")
	}

	dbClient := pit.DbClient(BaseURL)
	ctx := context.Background()

	_ = SetupTestTable(t, dbClient, ctx, "stress_rows")
	LoadTestData(t, dbClient, ctx, "stress_rows")

	// Test with increasing data sizes: 0.5x, 1x, 1.5x, 2x memory
	multipliers := []float64{0.5, 1.0, 1.5, 2.0}

	for _, mult := range multipliers {
		mult := mult
		testName := fmt.Sprintf("%.1fx_memory", mult)
		RunTracked(t, testName, func(t *testing.T) {
			targetBytes := int64(float64(DbMemoryBytes) * mult)
			exactDepth := calculateExponentialDepth(targetBytes)
			depth := int(math.Ceil(exactDepth))
			estimatedSize := estimateExponentialDataSize(depth)

			t.Logf("Target: %.1fx memory = %d bytes, exact depth=%.2f, using=%d, estimated=%.2f MB",
				mult, targetBytes, exactDepth, depth, float64(estimatedSize)/(1024*1024))

			replaceExpr := buildExponentialReplace(depth)
			sql := fmt.Sprintf("SELECT %s, id FROM stress_rows ORDER BY 0 ASC LIMIT 1", replaceExpr)

			runStressQuery(t, dbClient, ctx, sql, 5*time.Minute)
		})
	}
}

// ============================================================================
// CSE (Common Subexpression Elimination) Tests
// ============================================================================

// TestStress_CSE tests whether the database can eliminate common subexpressions.
// The same REPLACE chain appears 11 times:
//   - 10 times in SELECT: STRLEN(chain) + STRLEN(chain) + ... (10x)
//   - 1 time in WHERE: STRLEN(chain) > 0
//
// Without CSE: 11 * 0.1 = 1.1x memory needed (exceeds available!)
// With CSE: only 0.1x memory needed (computed once, reused)
func TestStress_CSE(t *testing.T) {
	RequireInterfaceVersion(t, 2)
	if DbMemoryBytes <= 0 {
		t.Skip("Skipping stress test: -db-memory flag not set")
	}

	dbClient := pit.DbClient(BaseURL)
	ctx := context.Background()

	_ = SetupTestTable(t, dbClient, ctx, "stress_rows")
	LoadTestData(t, dbClient, ctx, "stress_rows")

	// Target 10% of memory for a single expression (use Ceil to ensure test is valid)
	targetBytes := DbMemoryBytes / 10
	exactDepth := calculateExponentialDepth(targetBytes)
	depth := int(math.Ceil(exactDepth))
	estimatedSingleSize := estimateExponentialDataSize(depth)

	t.Logf("CSE test configuration:")
	t.Logf("  Database memory: %d bytes (%.2f MB)", DbMemoryBytes, float64(DbMemoryBytes)/(1024*1024))
	t.Logf("  Single expression target: %d bytes (%.2f MB) = 0.1x memory",
		targetBytes, float64(targetBytes)/(1024*1024))
	t.Logf("  Exact depth: %.2f, using Ceil: %d", exactDepth, depth)
	t.Logf("  Estimated single expr size: %d bytes (%.2f MB)",
		estimatedSingleSize, float64(estimatedSingleSize)/(1024*1024))
	t.Logf("  Without CSE would need: %.2f MB (11x expression = %.2fx memory - EXCEEDS!)",
		float64(estimatedSingleSize*11)/(1024*1024),
		float64(estimatedSingleSize*11)/float64(DbMemoryBytes))
	t.Logf("  With CSE should need: %.2f MB (1x expression = %.2fx memory)",
		float64(estimatedSingleSize)/(1024*1024),
		float64(estimatedSingleSize)/float64(DbMemoryBytes))

	// Build the query with the same expression used 11 times
	replaceExpr := buildExponentialReplace(depth)

	// SELECT: STRLEN(expr) + STRLEN(expr) + ... (10 times)
	// WHERE: STRLEN(expr) > 0 (always true)
	strlenExpr := fmt.Sprintf("STRLEN(%s)", replaceExpr)
	selectParts := make([]string, 10)
	for i := 0; i < 10; i++ {
		selectParts[i] = strlenExpr
	}
	selectExpr := strings.Join(selectParts, " + ")

	sql := fmt.Sprintf(
		"SELECT %s, id FROM stress_rows WHERE STRLEN(%s) > 0",
		selectExpr, replaceExpr)

	t.Logf("Query uses same REPLACE expression 11 times (10 in SELECT + 1 in WHERE)")
	t.Logf("Query length: %d chars", len(sql))

	runStressQuery(t, dbClient, ctx, sql, 5*time.Minute)
	t.Log("CSE test passed - database likely eliminated common subexpressions")
}

// TestStress_CSE_SelectOnly tests CSE with expressions only in SELECT clause.
// The same REPLACE chain appears 10 times in SELECT only.
//
// Without CSE: 10 * 0.1 = 1.0x memory needed (at the limit!)
// With CSE: only 0.1x memory needed (computed once, reused)
func TestStress_CSE_SelectOnly(t *testing.T) {
	RequireInterfaceVersion(t, 2)
	if DbMemoryBytes <= 0 {
		t.Skip("Skipping stress test: -db-memory flag not set")
	}

	dbClient := pit.DbClient(BaseURL)
	ctx := context.Background()

	_ = SetupTestTable(t, dbClient, ctx, "stress_rows")
	LoadTestData(t, dbClient, ctx, "stress_rows")

	// Target 10% of memory for a single expression (use Ceil)
	targetBytes := DbMemoryBytes / 10
	exactDepth := calculateExponentialDepth(targetBytes)
	depth := int(math.Ceil(exactDepth))
	estimatedSingleSize := estimateExponentialDataSize(depth)

	t.Logf("CSE (SELECT only) test configuration:")
	t.Logf("  Database memory: %d bytes (%.2f MB)", DbMemoryBytes, float64(DbMemoryBytes)/(1024*1024))
	t.Logf("  Single expression target: %d bytes (%.2f MB) = 0.1x memory",
		targetBytes, float64(targetBytes)/(1024*1024))
	t.Logf("  Exact depth: %.2f, using Ceil: %d", exactDepth, depth)
	t.Logf("  Estimated single expr size: %d bytes (%.2f MB)",
		estimatedSingleSize, float64(estimatedSingleSize)/(1024*1024))
	t.Logf("  Without CSE would need: %.2f MB (10x expression = %.2fx memory)",
		float64(estimatedSingleSize*10)/(1024*1024),
		float64(estimatedSingleSize*10)/float64(DbMemoryBytes))
	t.Logf("  With CSE should need: %.2f MB (1x expression = %.2fx memory)",
		float64(estimatedSingleSize)/(1024*1024),
		float64(estimatedSingleSize)/float64(DbMemoryBytes))

	// Build the query with the same expression used 10 times in SELECT only
	replaceExpr := buildExponentialReplace(depth)

	// SELECT: STRLEN(expr) + STRLEN(expr) + ... (10 times)
	strlenExpr := fmt.Sprintf("STRLEN(%s)", replaceExpr)
	selectParts := make([]string, 10)
	for i := 0; i < 10; i++ {
		selectParts[i] = strlenExpr
	}
	selectExpr := strings.Join(selectParts, " + ")

	sql := fmt.Sprintf("SELECT %s, id FROM stress_rows", selectExpr)

	t.Logf("Query uses same REPLACE expression 10 times (all in SELECT)")
	t.Logf("Query length: %d chars", len(sql))

	runStressQuery(t, dbClient, ctx, sql, 5*time.Minute)
	t.Log("CSE (SELECT only) test passed")
}
