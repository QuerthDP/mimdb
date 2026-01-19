package tests

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	apiclient "github.com/smogork/ISBD-MIMUW/pit/client/openapi2"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// Validation Test Runner (for testing query validation - pass/fail)
// ============================================================================

// ValidationTestCase represents a single validation test
type ValidationTestCase struct {
	Name                  string
	SQL                   string
	ExpectSuccess         bool
	ExpectedErrorContains string
}

// SubmittedValidation holds the result of a submitted validation query
type SubmittedValidation struct {
	TestCase     ValidationTestCase
	QueryID      string
	SubmitResp   *http.Response
	SubmitErr    error
	SubmitFailed bool
}

// ValidationTestRunner manages batch validation tests with sync/async support
type ValidationTestRunner struct {
	t         *testing.T
	apiClient *apiclient.APIClient
	ctx       context.Context
	cases     []ValidationTestCase
	submitted []SubmittedValidation
}

// NewValidationTestRunner creates a new validation test runner
func NewValidationTestRunner(t *testing.T, apiClient *apiclient.APIClient, ctx context.Context) *ValidationTestRunner {
	return &ValidationTestRunner{
		t:         t,
		apiClient: apiClient,
		ctx:       ctx,
	}
}

// AddSuccessCase adds a test case that should succeed
func (r *ValidationTestRunner) AddSuccessCase(name, sql string) {
	r.cases = append(r.cases, ValidationTestCase{
		Name:          name,
		SQL:           sql,
		ExpectSuccess: true,
	})
}

// AddFailureCase adds a test case that should fail validation
func (r *ValidationTestRunner) AddFailureCase(name, sql, expectedErrorContains string) {
	r.cases = append(r.cases, ValidationTestCase{
		Name:                  name,
		SQL:                   sql,
		ExpectSuccess:         false,
		ExpectedErrorContains: expectedErrorContains,
	})
}

// submitQuery submits a single query
func (r *ValidationTestRunner) submitQuery(tc ValidationTestCase) SubmittedValidation {
	queryId, resp, err := SubmitSelectQuery(r.apiClient, r.ctx, tc.SQL)

	result := SubmittedValidation{
		TestCase:   tc,
		QueryID:    queryId,
		SubmitResp: resp,
		SubmitErr:  err,
	}

	if resp != nil && resp.StatusCode == http.StatusBadRequest {
		result.SubmitFailed = true
	}

	return result
}

// RunSync runs all test cases synchronously (one by one)
func (r *ValidationTestRunner) RunSync() {
	for _, tc := range r.cases {
		tc := tc
		RunTracked(r.t, tc.Name, func(t *testing.T) {
			submitted := r.submitQuery(tc)
			if tc.ExpectSuccess {
				r.assertSuccess(t, submitted)
			} else {
				r.assertFailure(t, submitted, tc.ExpectedErrorContains)
			}
		})
	}
}

// RunAsync runs all test cases asynchronously (submit all, then verify all)
func (r *ValidationTestRunner) RunAsync() {
	if len(r.cases) == 0 {
		return
	}

	r.t.Log("=== ASYNC MODE: Submitting all validation queries ===")

	// Phase 1: Submit all queries
	r.submitted = make([]SubmittedValidation, len(r.cases))
	for i, tc := range r.cases {
		r.submitted[i] = r.submitQuery(tc)
		r.t.Logf("Submitted [%d/%d] %s: queryId=%s, failed=%v",
			i+1, len(r.cases), tc.Name, r.submitted[i].QueryID, r.submitted[i].SubmitFailed)
	}

	r.t.Log("=== ASYNC MODE: Waiting for results ===")

	// Phase 2: Wait and verify all results
	for i, submitted := range r.submitted {
		submitted := submitted
		RunTracked(r.t, submitted.TestCase.Name, func(t *testing.T) {
			t.Logf("Checking [%d/%d] %s", i+1, len(r.submitted), submitted.TestCase.Name)
			if submitted.TestCase.ExpectSuccess {
				r.assertSuccess(t, submitted)
			} else {
				r.assertFailure(t, submitted, submitted.TestCase.ExpectedErrorContains)
			}
		})
	}
}

// Run executes tests based on global AsyncMode flag
func (r *ValidationTestRunner) Run() {
	if AsyncMode {
		r.RunAsync()
	} else {
		r.RunSync()
	}
}

// assertSuccess checks that a query completed successfully
func (r *ValidationTestRunner) assertSuccess(t *testing.T, submitted SubmittedValidation) {
	if submitted.SubmitErr != nil {
		t.Fatalf("Query submission failed: %v", submitted.SubmitErr)
	}
	if submitted.SubmitFailed {
		t.Fatalf("Query submission returned 400, expected success")
	}
	require.Equal(t, http.StatusOK, submitted.SubmitResp.StatusCode)

	query, err := WaitForQueryCompletionWithFlush(r.apiClient, r.ctx, submitted.QueryID, 10*time.Second) // Flushing, because it can succeed and generate results
	require.NoError(t, err, "Query should complete within timeout")
	require.Equal(t, apiclient.COMPLETED, query.GetStatus(), "Query should complete successfully")
}

// assertFailure checks that a query failed validation
func (r *ValidationTestRunner) assertFailure(t *testing.T, submitted SubmittedValidation, expectedErrorContains string) {
	if submitted.SubmitFailed {
		t.Logf("Query failed at submission with 400 (expected)")
		return
	}

	if submitted.SubmitErr != nil {
		t.Fatalf("Unexpected submission error: %v", submitted.SubmitErr)
	}

	query, err := WaitForQueryCompletionWithFlush(r.apiClient, r.ctx, submitted.QueryID, 10*time.Second) // Flushing, because it can succeed and generate results
	require.NoError(t, err, "Query should complete within timeout")
	require.Equal(t, apiclient.FAILED, query.GetStatus(), "Query should fail validation")

	if expectedErrorContains != "" {
		problems, err := GetQueryError(r.apiClient, r.ctx, submitted.QueryID)
		require.NoError(t, err, "Should be able to get error details")
		require.NotEmpty(t, problems.Problems, "Should have error problems")

		found := false
		for _, p := range problems.Problems {
			if strings.Contains(strings.ToLower(p.Error), strings.ToLower(expectedErrorContains)) {
				found = true
				break
			}
		}
		if !found {
			t.Logf("Expected error containing '%s', got: %+v", expectedErrorContains, problems.Problems)
		}
	}
}

// ============================================================================
// Functional Test Runner (for testing query results correctness)
// ============================================================================

// FunctionalTestCase represents a test that verifies query results
type FunctionalTestCase struct {
	Name     string
	SQL      string
	Expected [][]interface{} // Expected rows, each row is a slice of column values
}

// SubmittedFunctional holds the result of a submitted functional query
type SubmittedFunctional struct {
	TestCase     FunctionalTestCase
	QueryID      string
	SubmitResp   *http.Response
	SubmitErr    error
	SubmitFailed bool
}

// FunctionalTestRunner manages batch functional tests with sync/async support
type FunctionalTestRunner struct {
	t         *testing.T
	apiClient *apiclient.APIClient
	ctx       context.Context
	cases     []FunctionalTestCase
	submitted []SubmittedFunctional
}

// NewFunctionalTestRunner creates a new functional test runner
func NewFunctionalTestRunner(t *testing.T, apiClient *apiclient.APIClient, ctx context.Context) *FunctionalTestRunner {
	return &FunctionalTestRunner{
		t:         t,
		apiClient: apiClient,
		ctx:       ctx,
	}
}

// AddCase adds a test case with expected results
func (r *FunctionalTestRunner) AddCase(name, sql string, expected [][]interface{}) {
	r.cases = append(r.cases, FunctionalTestCase{
		Name:     name,
		SQL:      sql,
		Expected: expected,
	})
}

// submitQuery submits a single query
func (r *FunctionalTestRunner) submitQuery(tc FunctionalTestCase) SubmittedFunctional {
	queryId, resp, err := SubmitSelectQuery(r.apiClient, r.ctx, tc.SQL)

	result := SubmittedFunctional{
		TestCase:   tc,
		QueryID:    queryId,
		SubmitResp: resp,
		SubmitErr:  err,
	}

	if resp != nil && resp.StatusCode == http.StatusBadRequest {
		result.SubmitFailed = true
	}

	return result
}

// RunSync runs all test cases synchronously (one by one)
func (r *FunctionalTestRunner) RunSync() {
	for _, tc := range r.cases {
		tc := tc
		RunTracked(r.t, tc.Name, func(t *testing.T) {
			submitted := r.submitQuery(tc)
			r.assertResult(t, submitted)
		})
	}
}

// RunAsync runs all test cases asynchronously (submit all, then verify all)
func (r *FunctionalTestRunner) RunAsync() {
	if len(r.cases) == 0 {
		return
	}

	r.t.Log("=== ASYNC MODE: Submitting all functional queries ===")

	// Phase 1: Submit all queries
	r.submitted = make([]SubmittedFunctional, len(r.cases))
	for i, tc := range r.cases {
		r.submitted[i] = r.submitQuery(tc)
		r.t.Logf("Submitted [%d/%d] %s: queryId=%s, failed=%v",
			i+1, len(r.cases), tc.Name, r.submitted[i].QueryID, r.submitted[i].SubmitFailed)
	}

	r.t.Log("=== ASYNC MODE: Waiting for results ===")

	// Phase 2: Wait and verify all results
	for i, submitted := range r.submitted {
		submitted := submitted
		RunTracked(r.t, submitted.TestCase.Name, func(t *testing.T) {
			t.Logf("Checking [%d/%d] %s", i+1, len(r.submitted), submitted.TestCase.Name)
			r.assertResult(t, submitted)
		})
	}
}

// Run executes tests based on global AsyncMode flag
func (r *FunctionalTestRunner) Run() {
	if AsyncMode {
		r.RunAsync()
	} else {
		r.RunSync()
	}
}

// assertResult checks that a query completed and returned expected results
func (r *FunctionalTestRunner) assertResult(t *testing.T, submitted SubmittedFunctional) {
	if submitted.SubmitErr != nil {
		t.Fatalf("Query submission failed: %v", submitted.SubmitErr)
	}
	if submitted.SubmitFailed {
		t.Fatalf("Query submission returned 400, expected success")
	}
	require.Equal(t, http.StatusOK, submitted.SubmitResp.StatusCode)

	// Wait for completion
	query, err := WaitForQueryCompletion(r.apiClient, r.ctx, submitted.QueryID, 10*time.Second)
	require.NoError(t, err, "Query should complete within timeout")

	if query.GetStatus() == apiclient.FAILED {
		problems, _ := GetQueryError(r.apiClient, r.ctx, submitted.QueryID)
		t.Fatalf("Query failed unexpectedly: %+v", problems)
	}
	require.Equal(t, apiclient.COMPLETED, query.GetStatus())

	// Get results
	results, err := GetQueryResult(r.apiClient, r.ctx, submitted.QueryID)
	require.NoError(t, err, "Should be able to get query results")

	// Parse and compare results
	actualRows := parseQueryResults(results)
	compareResults(t, submitted.TestCase.Expected, actualRows)
}

// ============================================================================
// Result Parsing and Comparison Helpers
// ============================================================================

// parseQueryResults converts API results to comparable format (row-based)
func parseQueryResults(results []apiclient.QueryResultInner) [][]interface{} {
	if len(results) == 0 {
		return nil
	}

	// Results are column-based, need to transpose to rows
	numCols := len(results[0].Columns)
	if numCols == 0 {
		return nil
	}

	// Get column values (each column is a oneOf type)
	columnValues := make([][]interface{}, numCols)
	for colIdx := 0; colIdx < numCols; colIdx++ {
		col := results[0].Columns[colIdx]
		columnValues[colIdx] = extractColumnValues(col)
	}

	// Handle empty results
	if len(columnValues[0]) == 0 {
		return [][]interface{}{}
	}

	numRows := len(columnValues[0])
	rows := make([][]interface{}, numRows)

	for rowIdx := 0; rowIdx < numRows; rowIdx++ {
		row := make([]interface{}, numCols)
		for colIdx := 0; colIdx < numCols; colIdx++ {
			row[colIdx] = columnValues[colIdx][rowIdx]
		}
		rows[rowIdx] = row
	}

	return rows
}

// extractColumnValues extracts values from a column (which is a oneOf type)
func extractColumnValues(col apiclient.QueryResultInnerColumnsInner) []interface{} {
	if col.ArrayOfBool != nil {
		vals := make([]interface{}, len(*col.ArrayOfBool))
		for i, v := range *col.ArrayOfBool {
			vals[i] = v
		}
		return vals
	}
	if col.ArrayOfInt64 != nil {
		vals := make([]interface{}, len(*col.ArrayOfInt64))
		for i, v := range *col.ArrayOfInt64 {
			vals[i] = v
		}
		return vals
	}
	if col.ArrayOfString != nil {
		vals := make([]interface{}, len(*col.ArrayOfString))
		for i, v := range *col.ArrayOfString {
			vals[i] = v
		}
		return vals
	}
	return nil
}

// compareResults compares expected and actual results
func compareResults(t *testing.T, expected, actual [][]interface{}) {
	require.Equal(t, len(expected), len(actual), "Row count mismatch")

	for i, expectedRow := range expected {
		actualRow := actual[i]
		require.Equal(t, len(expectedRow), len(actualRow), "Column count mismatch in row %d", i)

		for j, expectedVal := range expectedRow {
			actualVal := actualRow[j]
			compareValue(t, expectedVal, actualVal, i, j)
		}
	}
}

// compareValue compares a single expected vs actual value with type handling
func compareValue(t *testing.T, expected, actual interface{}, row, col int) {
	// Handle nil
	if expected == nil {
		require.Nil(t, actual, "Row %d, Col %d: expected nil", row, col)
		return
	}

	// Convert to comparable types (handles int64/int differences etc.)
	expectedStr := fmt.Sprintf("%v", expected)
	actualStr := fmt.Sprintf("%v", actual)

	require.Equal(t, expectedStr, actualStr, "Row %d, Col %d: value mismatch", row, col)
}
