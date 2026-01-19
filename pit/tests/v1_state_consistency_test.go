package tests

import (
	"context"
	"net/http"
	"testing"

	"github.com/smogork/ISBD-MIMUW/pit"
	openapi1 "github.com/smogork/ISBD-MIMUW/pit/client/openapi1"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// State Consistency Tests (Interface v1)
// ============================================================================

func TestV1_StateConsistency(t *testing.T) {
	RequireInterfaceVersion(t, 1)

	dbClient := DbClientV1(BaseURL)
	ctx := context.Background()

	RunTracked(t, "CreateDeleteCreate_SameName", func(t *testing.T) {
		schema, err := ReadTableSchemaV1("people")
		require.NoError(t, err)

		// Create first time
		t.Log(pit.FormatRequest("PUT", "/table", schema))
		tableId1, resp, err := dbClient.SchemaAPI.CreateTable(ctx).TableSchema(*schema).Execute()
		t.Log(pit.FormatResponse(resp))
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode)

		// Delete
		t.Logf("Sending request:\nDELETE /table/%s", tableId1)
		resp, err = dbClient.SchemaAPI.DeleteTable(ctx, tableId1).Execute()
		t.Log(pit.FormatResponse(resp))
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode)

		// Create again with same name - should succeed (with cleanup)
		tableId2 := CreateTableWithCleanupV1(t, dbClient, ctx, schema)
		require.NotEqual(t, tableId1, tableId2, "New table should have different ID")
	})

	RunTracked(t, "CopyThenSelect_DataImmediatelyVisible", func(t *testing.T) {
		_ = SetupTestTableV1(t, dbClient, ctx, "people")
		LoadTestDataV1(t, dbClient, ctx, "people")

		// Data should be immediately visible after COPY completes
		result := ExecuteSelectStarV1(t, dbClient, ctx, "people")
		rows := ParseQueryResultsV1(result)
		require.Len(t, rows, 5, "Data should be visible immediately after COPY")
	})

	RunTracked(t, "MultipleTables_Isolation", func(t *testing.T) {
		// Create two tables
		_ = SetupTestTableV1(t, dbClient, ctx, "people")
		_ = SetupTestTableV1(t, dbClient, ctx, "types_test")

		// Load data only to people
		LoadTestDataV1(t, dbClient, ctx, "people")

		// types_test should be empty
		result := ExecuteSelectStarV1(t, dbClient, ctx, "types_test")
		rows := ParseQueryResultsV1(result)
		require.Empty(t, rows, "types_test should be empty")

		// people should have data
		result = ExecuteSelectStarV1(t, dbClient, ctx, "people")
		rows = ParseQueryResultsV1(result)
		require.Len(t, rows, 5, "people should have 5 rows")
	})

	RunTracked(t, "DeleteTable_RemovesFromList", func(t *testing.T) {
		schema, err := ReadTableSchemaV1("people")
		require.NoError(t, err)

		// Create table with cleanup (cleanup handles 404 if already deleted)
		tableId := CreateTableV1(t, dbClient, ctx, schema)

		// Verify it's in the list
		t.Log("Sending request:\nGET /table")
		tables, resp, err := dbClient.SchemaAPI.GetTables(ctx).Execute()
		t.Log(pit.FormatResponse(resp))
		require.NoError(t, err)
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

		// Verify it's NOT in the list
		t.Log("Sending request:\nGET /table")
		tables, resp, err = dbClient.SchemaAPI.GetTables(ctx).Execute()
		t.Log(pit.FormatResponse(resp))
		for _, table := range tables {
			require.NotEqual(t, "people", table.GetName(),
				"Deleted table should not be in list")
		}
	})

	RunTracked(t, "DeleteTable_DetailsNotAccessible", func(t *testing.T) {
		schema, err := ReadTableSchemaV1("people")
		require.NoError(t, err)

		// Create table with cleanup (cleanup handles 404 if already deleted)
		tableId := CreateTableV1(t, dbClient, ctx, schema)

		// Delete table
		t.Logf("Sending request:\nDELETE /table/%s", tableId)
		resp, err := dbClient.SchemaAPI.DeleteTable(ctx, tableId).Execute()
		t.Log(pit.FormatResponse(resp))
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode)

		// Try to get details - should fail
		t.Logf("Sending request:\nGET /table/%s", tableId)
		_, resp, _ = dbClient.SchemaAPI.GetTableById(ctx, tableId).Execute()
		t.Log(pit.FormatResponse(resp))
		require.Equal(t, http.StatusNotFound, resp.StatusCode,
			"Deleted table details should return 404")
	})

	RunTracked(t, "MultipleQueries_SameTable", func(t *testing.T) {
		_ = SetupTestTableV1(t, dbClient, ctx, "types_test")
		LoadTestDataV1(t, dbClient, ctx, "types_test")

		// Execute multiple SELECT queries on same table
		for i := 0; i < 3; i++ {
			result := ExecuteSelectStarV1(t, dbClient, ctx, "types_test")
			rows := ParseQueryResultsV1(result)
			require.Len(t, rows, 3, "Each query should return same number of rows")
		}
	})

	RunTracked(t, "TableListConsistency", func(t *testing.T) {
		// Get initial table count
		t.Log("Sending request:\nGET /table")
		initialTables, resp, err := dbClient.SchemaAPI.GetTables(ctx).Execute()
		t.Log(pit.FormatResponse(resp))
		require.NoError(t, err)
		initialCount := len(initialTables)

		// Create table
		_ = SetupTestTableV1(t, dbClient, ctx, "people")

		// Verify count increased
		t.Log("Sending request:\nGET /table")
		tables, resp, err := dbClient.SchemaAPI.GetTables(ctx).Execute()
		t.Log(pit.FormatResponse(resp))
		require.NoError(t, err)
		require.Equal(t, initialCount+1, len(tables),
			"Table count should increase by 1 after creation")
	})
}

// ============================================================================
// Table Schema Consistency Tests (Interface v1)
// ============================================================================

func TestV1_TableSchemaConsistency(t *testing.T) {
	RequireInterfaceVersion(t, 1)

	dbClient := DbClientV1(BaseURL)
	ctx := context.Background()

	RunTracked(t, "Schema_MatchesCreatedSchema", func(t *testing.T) {
		schema, err := ReadTableSchemaV1("people")
		require.NoError(t, err)

		tableId := SetupTestTableV1(t, dbClient, ctx, "people")

		// Get table details
		t.Logf("Sending request:\nGET /table/%s", tableId)
		details, resp, err := dbClient.SchemaAPI.GetTableById(ctx, tableId).Execute()
		t.Log(pit.FormatResponse(resp))
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode)

		// Verify name matches
		require.Equal(t, schema.Name, details.Name)

		// Verify column count matches
		require.Equal(t, len(schema.Columns), len(details.Columns))
	})

	RunTracked(t, "Schema_ColumnTypes_Preserved", func(t *testing.T) {
		schema := &openapi1.TableSchema{
			Name: "test_types",
			Columns: []openapi1.Column{
				{Name: "int_col", Type: openapi1.INT64},
				{Name: "str_col", Type: openapi1.VARCHAR},
			},
		}

		tableId := CreateTableWithCleanupV1(t, dbClient, ctx, schema)

		// Get table details
		t.Logf("Sending request:\nGET /table/%s", tableId)
		details, resp, err := dbClient.SchemaAPI.GetTableById(ctx, tableId).Execute()
		t.Log(pit.FormatResponse(resp))
		require.NoError(t, err)

		// Find columns and verify types
		for _, expectedCol := range schema.Columns {
			found := false
			for _, actualCol := range details.Columns {
				if actualCol.Name == expectedCol.Name {
					found = true
					require.Equal(t, expectedCol.Type, actualCol.Type,
						"Column type should match for %s", expectedCol.Name)
					break
				}
			}
			require.True(t, found, "Column %s should exist", expectedCol.Name)
		}
	})
}
