package tests

import (
	"bufio"
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/smogork/ISBD-MIMUW/pit"
	openapi1 "github.com/smogork/ISBD-MIMUW/pit/client/openapi1"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// API Client for Interface v1
// ============================================================================

// DbClientV1 creates an API client for interface version 1
// This is an alias for pit.DbClient1 for convenience in tests
func DbClientV1(url string) *openapi1.APIClient {
	return pit.DbClient1(url)
}

// ============================================================================
// Table Schema Helpers for v1
// ============================================================================

// ReadTableSchemaV1 reads a table schema from the tables directory for v1 client
func ReadTableSchemaV1(tableName string) (*openapi1.TableSchema, error) {
	schemaPath := filepath.Join("..", "tables", tableName, "schema.txt")
	file, err := os.Open(schemaPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open schema file: %w", err)
	}
	defer file.Close()

	var columns []openapi1.Column
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		parts := strings.Fields(line)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid schema line: %s", line)
		}

		colName := parts[0]
		colType := openapi1.LogicalColumnType(parts[1])
		columns = append(columns, openapi1.Column{Name: colName, Type: colType})
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading schema file: %w", err)
	}

	return openapi1.NewTableSchema(tableName, columns), nil
}

// CreateTableWithCleanupV1 creates a table with custom schema and registers cleanup for v1 client
func CreateTableWithCleanupV1(t *testing.T, apiClient *openapi1.APIClient, ctx context.Context, schema *openapi1.TableSchema) string {
	t.Log(pit.FormatRequest("PUT", "/table", schema))
	tableId, resp, err := apiClient.SchemaAPI.CreateTable(ctx).TableSchema(*schema).Execute()
	t.Log(pit.FormatResponse(resp))

	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NotEmpty(t, tableId)

	t.Cleanup(func() {
		t.Logf("Sending request:\nDELETE /table/%s", tableId)
		resp, err := apiClient.SchemaAPI.DeleteTable(ctx, tableId).Execute()
		t.Log(pit.FormatResponse(resp))

		// 404 is OK - table may have been deleted as part of the test
		if resp.StatusCode != http.StatusOK {
			t.Errorf("Cleanup failed: Could not delete table %s: %v, status: %d", tableId, err, resp.StatusCode)
		}
	})

	return tableId
}

// CreateTableV1 creates a table with custom schema for v1 client
func CreateTableV1(t *testing.T, apiClient *openapi1.APIClient, ctx context.Context, schema *openapi1.TableSchema) string {
	t.Log(pit.FormatRequest("PUT", "/table", schema))
	tableId, resp, err := apiClient.SchemaAPI.CreateTable(ctx).TableSchema(*schema).Execute()
	t.Log(pit.FormatResponse(resp))

	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NotEmpty(t, tableId)

	return tableId
}

// SetupTestTableV1 creates a table and registers cleanup for v1 client
func SetupTestTableV1(t *testing.T, apiClient *openapi1.APIClient, ctx context.Context, tableName string) string {
	schema, err := ReadTableSchemaV1(tableName)
	require.NoError(t, err, "Failed to read schema for table %s", tableName)

	t.Log(pit.FormatRequest("PUT", "/table", schema))
	tableId, resp, err := apiClient.SchemaAPI.CreateTable(ctx).TableSchema(*schema).Execute()
	t.Log(pit.FormatResponse(resp))
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	t.Cleanup(func() {
		t.Logf("Sending request:\nDELETE /table/%s", tableId)
		resp, err := apiClient.SchemaAPI.DeleteTable(ctx, tableId).Execute()
		t.Log(pit.FormatResponse(resp))
		// 404 is OK - table may have been deleted as part of the test
		if resp.StatusCode != http.StatusOK {
			t.Errorf("Cleanup failed: Could not delete table %s: %v", tableId, err)
		}
	})

	return tableId
}

// ============================================================================
// COPY Query Helpers for v1
// ============================================================================

// LoadTestDataV1 loads CSV data into a table using COPY query for v1 client
func LoadTestDataV1(t *testing.T, apiClient *openapi1.APIClient, ctx context.Context, tableName string) {
	dataPath := fmt.Sprintf("/data/tables/%s/data.csv", tableName)

	copyQuery := &openapi1.CopyQuery{
		SourceFilepath:       dataPath,
		DestinationTableName: tableName,
	}
	doesContainHeader := true
	copyQuery.DoesCsvContainHeader = &doesContainHeader

	queryDef := openapi1.CopyQueryAsQueryQueryDefinition(copyQuery)
	req := openapi1.ExecuteQueryRequest{QueryDefinition: queryDef}

	t.Log(pit.FormatRequest("POST", "/query", copyQuery))
	queryId, resp, err := apiClient.ExecutionAPI.SubmitQuery(ctx).ExecuteQueryRequest(req).Execute()
	t.Log(pit.FormatResponse(resp))
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Wait for COPY to complete
	query, err := WaitForQueryCompletionV1(apiClient, ctx, queryId, 30*time.Second)
	require.NoError(t, err, "COPY query should complete")
	require.Equal(t, openapi1.COMPLETED, query.GetStatus(), "COPY query should succeed")
}

// LoadTestDataV1 loads CSV data into a table using COPY query for v1 client
func LoadTestDataWithMappingV1(t *testing.T, apiClient *openapi1.APIClient, ctx context.Context, tableName string, columnMapping []string) {
	dataPath := fmt.Sprintf("/data/tables/%s/data.csv", tableName)

	copyQuery := &openapi1.CopyQuery{
		SourceFilepath:       dataPath,
		DestinationTableName: tableName,
		DestinationColumns:   columnMapping,
	}
	doesContainHeader := true
	copyQuery.DoesCsvContainHeader = &doesContainHeader

	queryDef := openapi1.CopyQueryAsQueryQueryDefinition(copyQuery)
	req := openapi1.ExecuteQueryRequest{QueryDefinition: queryDef}

	t.Log(pit.FormatRequest("POST", "/query", copyQuery))
	queryId, resp, err := apiClient.ExecutionAPI.SubmitQuery(ctx).ExecuteQueryRequest(req).Execute()
	t.Log(pit.FormatResponse(resp))
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Wait for COPY to complete
	query, err := WaitForQueryCompletionV1(apiClient, ctx, queryId, 30*time.Second)
	require.NoError(t, err, "COPY query should complete")
	require.Equal(t, openapi1.COMPLETED, query.GetStatus(), "COPY query should succeed")
}

// LoadTestDataV1 loads CSV data into a table using COPY query for v1 client
func LoadTestDataHeadlessV1(t *testing.T, apiClient *openapi1.APIClient, ctx context.Context, tableName string) {
	dataPath := fmt.Sprintf("/data/tables/%s/data-headless.csv", tableName)

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
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Wait for COPY to complete
	query, err := WaitForQueryCompletionV1(apiClient, ctx, queryId, 30*time.Second)
	require.NoError(t, err, "COPY query should complete")
	require.Equal(t, openapi1.COMPLETED, query.GetStatus(), "COPY query should succeed")
}

// SubmitCopyQueryV1 submits a COPY query and returns query ID
func SubmitCopyQueryV1(apiClient *openapi1.APIClient, ctx context.Context, sourcePath, destTable string, hasHeader bool) (string, *http.Response, error) {
	copyQuery := &openapi1.CopyQuery{
		SourceFilepath:       sourcePath,
		DestinationTableName: destTable,
	}
	copyQuery.DoesCsvContainHeader = &hasHeader

	queryDef := openapi1.CopyQueryAsQueryQueryDefinition(copyQuery)
	req := openapi1.ExecuteQueryRequest{QueryDefinition: queryDef}

	return apiClient.ExecutionAPI.SubmitQuery(ctx).ExecuteQueryRequest(req).Execute()
}

// ============================================================================
// SELECT Query Helpers for v1
// ============================================================================

// SubmitSelectStarQueryV1 submits a SELECT * query for v1 client
func SubmitSelectStarQueryV1(apiClient *openapi1.APIClient, ctx context.Context, tableName string) (string, *http.Response, error) {
	selectQuery := &openapi1.SelectQuery{}
	selectQuery.SetTableName(tableName)

	queryDef := openapi1.SelectQueryAsQueryQueryDefinition(selectQuery)
	req := openapi1.ExecuteQueryRequest{QueryDefinition: queryDef}

	return apiClient.ExecutionAPI.SubmitQuery(ctx).ExecuteQueryRequest(req).Execute()
}

// ExecuteSelectStarV1 executes SELECT * and returns results for v1 client
func ExecuteSelectStarV1(t *testing.T, apiClient *openapi1.APIClient, ctx context.Context, tableName string) []openapi1.QueryResultInner {
	selectQuery := &openapi1.SelectQuery{}
	selectQuery.SetTableName(tableName)

	t.Log(pit.FormatRequest("POST", "/query", selectQuery))
	queryId, resp, err := SubmitSelectStarQueryV1(apiClient, ctx, tableName)
	t.Log(pit.FormatResponse(resp))
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	query, err := WaitForQueryCompletionV1(apiClient, ctx, queryId, 30*time.Second)
	require.NoError(t, err)
	require.Equal(t, openapi1.COMPLETED, query.GetStatus())

	t.Logf("Sending request:\nGET /result/%s", queryId)
	result, err := GetQueryResultV1(apiClient, ctx, queryId)
	require.NoError(t, err)
	return result
}

// ============================================================================
// Query Execution Helpers for v1
// ============================================================================

// WaitForQueryCompletionV1 waits for a query to reach COMPLETED or FAILED status
func WaitForQueryCompletionV1(apiClient *openapi1.APIClient, ctx context.Context, queryId string, timeout time.Duration) (*openapi1.Query, error) {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		query, _, err := apiClient.ExecutionAPI.GetQueryById(ctx, queryId).Execute()
		if err != nil {
			return nil, fmt.Errorf("failed to get query status: %w", err)
		}

		status := query.GetStatus()
		if status == openapi1.COMPLETED || status == openapi1.FAILED {
			// Log error details if query failed
			if status == openapi1.FAILED {
				if problems, err := GetQueryErrorV1(apiClient, ctx, queryId); err == nil && problems != nil {
					fmt.Printf("Query %s FAILED with errors:\n", queryId)
					for _, p := range problems.Problems {
						fmt.Printf("  - %s\n", p.Error)
					}
				}
			}
			return query, nil
		}

		time.Sleep(50 * time.Millisecond)
	}

	return nil, fmt.Errorf("timeout waiting for query %s to complete", queryId)
}

// WaitForQueryCompletionWithFlushV1 waits for completion and flushes result
func WaitForQueryCompletionWithFlushV1(apiClient *openapi1.APIClient, ctx context.Context, queryId string, timeout time.Duration) (*openapi1.Query, error) {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		query, _, err := apiClient.ExecutionAPI.GetQueryById(ctx, queryId).Execute()
		if err != nil {
			return nil, fmt.Errorf("failed to get query status: %w", err)
		}

		status := query.GetStatus()
		if status == openapi1.COMPLETED || status == openapi1.FAILED {
			// Log error details if query failed
			if status == openapi1.FAILED {
				if problems, err := GetQueryErrorV1(apiClient, ctx, queryId); err == nil && problems != nil {
					fmt.Printf("Query %s FAILED with errors:\n", queryId)
					for _, p := range problems.Problems {
						fmt.Printf("  - %s\n", p.Error)
					}
				}
			}

			// Flush result to release DB resources (ignore errors)
			flushReq := openapi1.GetQueryResultRequest{}
			flushReq.SetFlushResult(true)
			_, _, _ = apiClient.ExecutionAPI.GetQueryResult(ctx, queryId).GetQueryResultRequest(flushReq).Execute()

			return query, nil
		}

		time.Sleep(50 * time.Millisecond)
	}

	return nil, fmt.Errorf("timeout waiting for query %s to complete", queryId)
}

// GetQueryResultV1 fetches the result of a completed query and flushes it
func GetQueryResultV1(apiClient *openapi1.APIClient, ctx context.Context, queryId string) ([]openapi1.QueryResultInner, error) {
	req := openapi1.GetQueryResultRequest{}
	req.SetFlushResult(true)
	result, _, err := apiClient.ExecutionAPI.GetQueryResult(ctx, queryId).GetQueryResultRequest(req).Execute()
	return result, err
}

// GetQueryErrorV1 retrieves error details for a failed query
func GetQueryErrorV1(apiClient *openapi1.APIClient, ctx context.Context, queryId string) (*openapi1.MultipleProblemsError, error) {
	problems, _, err := apiClient.ExecutionAPI.GetQueryError(ctx, queryId).Execute()
	return problems, err
}

// ============================================================================
// Result Parsing Helpers for v1
// ============================================================================

// ParseQueryResultsV1 converts API results to comparable format (row-based)
func ParseQueryResultsV1(results []openapi1.QueryResultInner) [][]interface{} {
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
		columnValues[colIdx] = extractColumnValuesV1(col)
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

// extractColumnValuesV1 extracts values from a column (which is a oneOf type)
func extractColumnValuesV1(col openapi1.QueryResultInnerColumnsInner) []interface{} {
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

// CountRowsV1 counts rows in query result
func CountRowsV1(result []openapi1.QueryResultInner) int {
	rows := ParseQueryResultsV1(result)
	return len(rows)
}
