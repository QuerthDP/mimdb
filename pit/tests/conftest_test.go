package tests

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/smogork/ISBD-MIMUW/pit"
	apiclient "github.com/smogork/ISBD-MIMUW/pit/client"
	"github.com/smogork/ISBD-MIMUW/pit/parser"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// Global Configuration
// ============================================================================

var BaseURL string

// AsyncMode controls whether tests run in async mode (submit all, then wait)
var AsyncMode bool

// DbMemoryBytes is the available memory in the database for stress tests (in bytes)
// When set > 0, stress tests will try to sort 2x this amount of data
var DbMemoryBytes int64

// Command-line flags for test configuration
var (
	dbImage      = flag.String("db-image", "", "Docker image name (env: DB_IMAGE, default: isbd-mimuw-db:latest)")
	dbHostname   = flag.String("db-hostname", "", "Hostname of running database (env: DB_HOSTNAME, default: localhost)")
	dbPort       = flag.String("db-port", "", "Port on which database listens (env: DB_PORT, default: 8080)")
	dbRunDocker  = flag.String("db-run-docker", "", "Skip docker container and use existing database (env: DB_RUN_DOCKER, default: false)")
	asyncFlag    = flag.Bool("async", false, "Run tests in async mode: submit all queries first, then wait for results")
	dbMemoryFlag = flag.Int64("db-memory", 0, "Database available memory in bytes for stress tests (0 = skip stress tests)")
)

func applyFlagToEnv() {
	if *dbImage != "" {
		os.Setenv("DB_IMAGE", *dbImage)
	}
	if *dbHostname != "" {
		os.Setenv("DB_HOSTNAME", *dbHostname)
	}
	if *dbPort != "" {
		os.Setenv("DB_PORT", *dbPort)
	}
	if *dbRunDocker != "" {
		os.Setenv("DB_RUN_DOCKER", *dbRunDocker)
	}
}

func TestMain(m *testing.M) {
	flag.Parse()
	applyFlagToEnv()

	// Set AsyncMode from flag or environment variable
	AsyncMode = *asyncFlag
	if !AsyncMode {
		if envAsync := os.Getenv("TEST_ASYNC"); envAsync == "true" || envAsync == "1" {
			AsyncMode = true
		}
	}

	// Set DbMemoryBytes from flag or environment variable
	DbMemoryBytes = *dbMemoryFlag
	if DbMemoryBytes == 0 {
		if envMemory := os.Getenv("DB_MEMORY"); envMemory != "" {
			if parsed, err := strconv.ParseInt(envMemory, 10, 64); err == nil {
				DbMemoryBytes = parsed
			}
		}
	}

	ctx := context.Background()
	base, teardown, err := pit.StartTestContainer(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, "failed to start test container:", err)
		os.Exit(1)
	}
	BaseURL = base

	code := m.Run()

	if teardown != nil {
		teardown()
	}

	os.Exit(code)
}

// ============================================================================
// Table Schema Helpers
// ============================================================================

// ReadTableSchema reads a table schema from the tables directory
func ReadTableSchema(tableName string) (*apiclient.TableSchema, error) {
	schemaPath := filepath.Join("..", "tables", tableName, "schema.txt")
	file, err := os.Open(schemaPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open schema file: %w", err)
	}
	defer file.Close()

	var columns []apiclient.Column
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
		colType := apiclient.LogicalColumnType(parts[1])
		columns = append(columns, apiclient.Column{Name: colName, Type: colType})
	}

	sort.Slice(columns, func(i, j int) bool {
		return columns[i].Name < columns[j].Name
	})

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading schema file: %w", err)
	}

	return apiclient.NewTableSchema(tableName, columns), nil
}

// SetupTestTable creates a table and registers cleanup
func SetupTestTable(t *testing.T, apiClient *apiclient.APIClient, ctx context.Context, tableName string) string {
	schema, err := ReadTableSchema(tableName)
	require.NoError(t, err, "Failed to read schema for table %s", tableName)

	tableId, resp, err := apiClient.SchemaAPI.CreateTable(ctx).TableSchema(*schema).Execute()
	t.Log(pit.FormatResponse(resp))
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	t.Cleanup(func() {
		resp, err := apiClient.SchemaAPI.DeleteTable(ctx, tableId).Execute()
		t.Log(pit.FormatResponse(resp))
		if err != nil || resp.StatusCode != http.StatusOK {
			t.Errorf("Cleanup failed: Could not delete table %s: %v", tableId, err)
		}
	})

	return tableId
}

// LoadTestData loads CSV data into a table using COPY query
func LoadTestData(t *testing.T, apiClient *apiclient.APIClient, ctx context.Context, tableName string) {
	dataPath, err := filepath.Abs(filepath.Join("..", "tables", tableName, "data.csv"))
	require.NoError(t, err)

	copyQuery := &apiclient.CopyQuery{
		SourceFilepath:       dataPath,
		DestinationTableName: tableName,
	}
	doesContainHeader := true
	copyQuery.DoesCsvContainHeader = &doesContainHeader

	queryDef := apiclient.CopyQueryAsQueryQueryDefinition(copyQuery)
	req := apiclient.ExecuteQueryRequest{QueryDefinition: queryDef}

	queryId, resp, err := apiClient.ExecutionAPI.SubmitQuery(ctx).ExecuteQueryRequest(req).Execute()
	t.Log(pit.FormatResponse(resp))
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Wait for COPY to complete
	_, err = WaitForQueryCompletion(apiClient, ctx, queryId, 30*time.Second)
	require.NoError(t, err, "COPY query should complete")
}

// ============================================================================
// Query Execution Helpers
// ============================================================================

// SubmitSelectQuery parses SQL and submits it, returning query ID
func SubmitSelectQuery(apiClient *apiclient.APIClient, ctx context.Context, sql string) (string, *http.Response, error) {
	selectQuery, err := parser.ParseSQL(sql)
	if err != nil {
		return "", nil, fmt.Errorf("failed to parse SQL: %w", err)
	}

	queryDef := apiclient.SelectQueryAsQueryQueryDefinition(selectQuery)
	req := apiclient.ExecuteQueryRequest{QueryDefinition: queryDef}

	return apiClient.ExecutionAPI.SubmitQuery(ctx).ExecuteQueryRequest(req).Execute()
}

// WaitForQueryCompletion waits for a query to reach COMPLETED or FAILED status
// After completion, it flushes the query result to release database resources
func WaitForQueryCompletion(apiClient *apiclient.APIClient, ctx context.Context, queryId string, timeout time.Duration) (*apiclient.Query, error) {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		query, _, err := apiClient.ExecutionAPI.GetQueryById(ctx, queryId).Execute()
		if err != nil {
			return nil, fmt.Errorf("failed to get query status: %w", err)
		}

		status := query.GetStatus()
		if status == apiclient.COMPLETED || status == apiclient.FAILED {
			// Flush result to release DB resources (ignore errors)
			flushReq := apiclient.GetQueryResultRequest{}
			flushReq.SetFlushResult(true)
			_, _, _ = apiClient.ExecutionAPI.GetQueryResult(ctx, queryId).GetQueryResultRequest(flushReq).Execute()

			return query, nil
		}

		time.Sleep(50 * time.Millisecond)
	}

	return nil, fmt.Errorf("timeout waiting for query %s to complete", queryId)
}

// GetQueryResult fetches the result of a completed query and flushes it
func GetQueryResult(apiClient *apiclient.APIClient, ctx context.Context, queryId string) ([]apiclient.QueryResultInner, error) {
	req := apiclient.GetQueryResultRequest{}
	req.SetFlushResult(true)
	result, _, err := apiClient.ExecutionAPI.GetQueryResult(ctx, queryId).GetQueryResultRequest(req).Execute()
	return result, err
}

// GetQueryError retrieves error details for a failed query
func GetQueryError(apiClient *apiclient.APIClient, ctx context.Context, queryId string) (*apiclient.MultipleProblemsError, error) {
	problems, _, err := apiClient.ExecutionAPI.GetQueryError(ctx, queryId).Execute()
	return problems, err
}
