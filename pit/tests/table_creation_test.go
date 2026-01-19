package tests

import (
	"bufio"
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/smogork/ISBD-MIMUW/pit"
	openapi1 "github.com/smogork/ISBD-MIMUW/pit/client/openapi1"
	"github.com/stretchr/testify/require"
)

func readPeopleSchema() (*openapi1.TableSchema, error) {
	// Get the path to the schema file
	schemaPath := filepath.Join("..", "tables", "people", "schema.txt")
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

	sort.Slice(columns, func(i, j int) bool {
		return columns[i].Name < columns[j].Name
	})

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading schema file: %w", err)
	}

	return openapi1.NewTableSchema("people", columns), nil
}

func createTable(t *testing.T, apiClient *openapi1.APIClient, ctx context.Context, tableSchema *openapi1.TableSchema, mayFail bool) (string, *http.Response, error) {
	t.Log(pit.FormatRequest("PUT", "/table", tableSchema))
	tableId, resp, err := apiClient.SchemaAPI.CreateTable(ctx).TableSchema(*tableSchema).Execute()
	t.Log(pit.FormatResponse(resp))
	if !mayFail {
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode)
	}
	return tableId, resp, err
}

func deleteTable(t *testing.T, apiClient *openapi1.APIClient, ctx context.Context, tableId string) (*http.Response, error) {
	t.Logf("Sending request:\nDELETE /table/%s", tableId)
	resp, err := apiClient.SchemaAPI.DeleteTable(ctx, tableId).Execute()
	t.Log(pit.FormatResponse(resp))
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	return resp, err
}

// createTableWithCleanup is an alias for CreateTableWithCleanupV1 for backward compatibility
func createTableWithCleanup(t *testing.T, apiClient *openapi1.APIClient, ctx context.Context, schema *openapi1.TableSchema) string {
	return CreateTableWithCleanupV1(t, apiClient, ctx, schema)
}

func TestTableCreation(t *testing.T) {
	dbClient := pit.DbClient1(BaseURL)
	ctx := context.Background()

	// Read the people table schema from file
	peopleSchema, err := readPeopleSchema()
	require.NoError(t, err)

	RunTracked(t, "TableEmptyList", func(t *testing.T) {
		t.Log("Sending request:\nGET /tables")
		tables, resp, err := dbClient.SchemaAPI.GetTables(ctx).Execute()
		t.Log(pit.FormatResponse(resp))
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode)
		require.Len(t, tables, 0)
	})

	RunTracked(t, "TableCreationAndList", func(t *testing.T) {
		// Create the people table
		_ = createTableWithCleanup(t, dbClient, ctx, peopleSchema)

		// List tables and verify the table exists
		t.Log("Sending request:\nGET /tables")
		tables, resp, err := dbClient.SchemaAPI.GetTables(ctx).Execute()
		t.Log(pit.FormatResponse(resp))
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode)
		require.Len(t, tables, 1)
		require.Equal(t, "people", tables[0].Name)
	})

	RunTracked(t, "TableCreationAndDetails", func(t *testing.T) {
		// Create the people table
		tableId := createTableWithCleanup(t, dbClient, ctx, peopleSchema)

		// Get table details
		t.Logf("Sending request:\nGET /table/%s", tableId)
		details, resp, err := dbClient.SchemaAPI.GetTableById(ctx, tableId).Execute()
		t.Log(pit.FormatResponse(resp))
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode)
		require.Equal(t, "people", details.Name)
		require.Len(t, details.Columns, 4)

		// Sort columns by name to make sure this will relaly be equal
		sort.Slice(details.Columns, func(i, j int) bool {
			return details.Columns[i].Name < details.Columns[j].Name
		})
		require.Equal(t, *peopleSchema, *details)
	})

	RunTracked(t, "TableDoubleCreation", func(t *testing.T) {
		// Create the people table
		_ = createTableWithCleanup(t, dbClient, ctx, peopleSchema)

		peopleSchema2, err := readPeopleSchema()
		require.NoError(t, err)
		peopleSchema2.Name = "people2"

		// Create another table with the same schema but different name - should succeed
		_ = createTableWithCleanup(t, dbClient, ctx, peopleSchema2)
	})

	RunTracked(t, "TableDoubleNameCreation", func(t *testing.T) {
		// Create the people table
		_ = createTableWithCleanup(t, dbClient, ctx, peopleSchema)

		// Try to create a table with the same name - should fail
		_, resp, _ := createTable(t, dbClient, ctx, peopleSchema, true)
		t.Log(pit.FormatResponse(resp))
		require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	})

	RunTracked(t, "TableDoubleRemove", func(t *testing.T) {
		// Create the people table
		tableId, _, _ := createTable(t, dbClient, ctx, peopleSchema, false)

		// Delete the table - should succeed
		_, _ = deleteTable(t, dbClient, ctx, tableId)

		// Try to delete the table again - should fail
		t.Logf("Sending request:\nDELETE /table/%s", tableId)
		resp, _ := dbClient.SchemaAPI.DeleteTable(ctx, tableId).Execute()
		t.Log(pit.FormatResponse(resp))
		require.Equal(t, http.StatusNotFound, resp.StatusCode)
	})

	RunTracked(t, "TableNoDetailsAfterRemoval", func(t *testing.T) {
		// Create the people table
		tableId, _, _ := createTable(t, dbClient, ctx, peopleSchema, false)

		// Delete the table - should succeed
		_, _ = deleteTable(t, dbClient, ctx, tableId)

		// Try to get details of deleted table - should fail
		t.Logf("Sending request:\nGET /table/%s", tableId)
		_, resp, _ := dbClient.SchemaAPI.GetTableById(ctx, tableId).Execute()
		t.Log(pit.FormatResponse(resp))
		require.Equal(t, http.StatusNotFound, resp.StatusCode)
	})
}

// ============================================================================
// CREATE TABLE - Atomicity Tests
// ============================================================================

func TestTableCreation_Atomicity(t *testing.T) {
	dbClient := pit.DbClient1(BaseURL)
	ctx := context.Background()

	RunTracked(t, "CreateWithInvalidColumnType_NoPartialState", func(t *testing.T) {
		// Schema with invalid column type
		invalidSchema := &openapi1.TableSchema{
			Name: "invalid_type_table",
			Columns: []openapi1.Column{
				{Name: "col1", Type: "INVALID_TYPE"},
			},
		}

		t.Log(pit.FormatRequest("PUT", "/table", invalidSchema))
		_, resp, _ := dbClient.SchemaAPI.CreateTable(ctx).TableSchema(*invalidSchema).Execute()
		t.Log(pit.FormatResponse(resp))
		require.NotEqual(t, http.StatusOK, resp.StatusCode)

		// Verify table does NOT exist (no partial state)
		t.Log("Sending request:\nGET /tables")
		tables, _, err := dbClient.SchemaAPI.GetTables(ctx).Execute()
		require.NoError(t, err)
		for _, table := range tables {
			require.NotEqual(t, "invalid_type_table", table.Name,
				"Table should not exist after failed creation")
		}
	})

	RunTracked(t, "CreateWithEmptyName_Fails", func(t *testing.T) {
		schema := &openapi1.TableSchema{
			Name: "",
			Columns: []openapi1.Column{
				{Name: "col1", Type: openapi1.INT64},
			},
		}

		t.Log(pit.FormatRequest("PUT", "/table", schema))
		_, resp, _ := dbClient.SchemaAPI.CreateTable(ctx).TableSchema(*schema).Execute()
		t.Log(pit.FormatResponse(resp))
		require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	})

	RunTracked(t, "CreateWithEmptyColumns_Fails", func(t *testing.T) {
		schema := &openapi1.TableSchema{
			Name:    "empty_cols_table",
			Columns: []openapi1.Column{},
		}

		t.Log(pit.FormatRequest("PUT", "/table", schema))
		_, resp, _ := dbClient.SchemaAPI.CreateTable(ctx).TableSchema(*schema).Execute()
		t.Log(pit.FormatResponse(resp))
		require.Equal(t, http.StatusBadRequest, resp.StatusCode)

		// Verify table does NOT exist
		t.Log("Sending request:\nGET /tables")
		tables, _, err := dbClient.SchemaAPI.GetTables(ctx).Execute()
		require.NoError(t, err)
		for _, table := range tables {
			require.NotEqual(t, "empty_cols_table", table.Name,
				"Table should not exist after failed creation")
		}
	})

	RunTracked(t, "CreateWithDuplicateColumnNames_Fails", func(t *testing.T) {
		schema := &openapi1.TableSchema{
			Name: "dup_cols_table",
			Columns: []openapi1.Column{
				{Name: "col1", Type: openapi1.INT64},
				{Name: "col1", Type: openapi1.VARCHAR}, // Duplicate!
			},
		}

		t.Log(pit.FormatRequest("PUT", "/table", schema))
		_, resp, _ := dbClient.SchemaAPI.CreateTable(ctx).TableSchema(*schema).Execute()
		t.Log(pit.FormatResponse(resp))
		require.Equal(t, http.StatusBadRequest, resp.StatusCode)

		// Verify table does NOT exist
		t.Log("Sending request:\nGET /tables")
		tables, _, err := dbClient.SchemaAPI.GetTables(ctx).Execute()
		require.NoError(t, err)
		for _, table := range tables {
			require.NotEqual(t, "dup_cols_table", table.Name,
				"Table should not exist after failed creation")
		}
	})

	RunTracked(t, "DeleteNonExistent_ReturnsNotFound", func(t *testing.T) {
		t.Log("Sending request:\nDELETE /table/non_existent_id_12345")
		resp, _ := dbClient.SchemaAPI.DeleteTable(ctx, "non_existent_id_12345").Execute()
		t.Log(pit.FormatResponse(resp))
		require.Equal(t, http.StatusNotFound, resp.StatusCode)
	})

	RunTracked(t, "DeleteById_NotByName", func(t *testing.T) {
		// Create a table with cleanup
		schema, err := readPeopleSchema()
		require.NoError(t, err)

		tableId := createTableWithCleanup(t, dbClient, ctx, schema)

		// Try to delete by name instead of ID - should fail
		t.Log("Sending request:\nDELETE /table/people")
		resp, _ := dbClient.SchemaAPI.DeleteTable(ctx, "people").Execute()
		t.Log(pit.FormatResponse(resp))
		require.Equal(t, http.StatusNotFound, resp.StatusCode)

		// Table should still exist
		t.Logf("Sending request:\nGET /table/%s", tableId)
		_, resp, err = dbClient.SchemaAPI.GetTableById(ctx, tableId).Execute()
		t.Log(pit.FormatResponse(resp))
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode)
	})

	RunTracked(t, "AfterDelete_NotInList", func(t *testing.T) {
		schema, err := readPeopleSchema()
		require.NoError(t, err)

		// Create with cleanup (cleanup handles 404 if already deleted)
		tableId, resp, err := createTable(t, dbClient, ctx, schema, false)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode)

		// Verify table is in list
		t.Log("Sending request:\nGET /tables")
		tables, resp, _ := dbClient.SchemaAPI.GetTables(ctx).Execute()
		t.Log(pit.FormatResponse(resp))
		found := false
		for _, table := range tables {
			if table.GetName() == "people" {
				found = true
				break
			}
		}
		require.True(t, found, "Table should be in list after creation")

		// Delete table
		t.Logf("Sending request:\nDELETE /table/%s", tableId)
		resp, err = dbClient.SchemaAPI.DeleteTable(ctx, tableId).Execute()
		t.Log(pit.FormatResponse(resp))
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode)

		// Verify table is NOT in list
		t.Log("Sending request:\nGET /tables")
		tables, resp, _ = dbClient.SchemaAPI.GetTables(ctx).Execute()
		t.Log(pit.FormatResponse(resp))
		for _, table := range tables {
			require.NotEqual(t, "people", table.GetName(),
				"Deleted table should not be in list")
		}
	})
}
