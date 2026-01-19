package tests

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/smogork/ISBD-MIMUW/pit"
	openapi1 "github.com/smogork/ISBD-MIMUW/pit/client/openapi1"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// SELECT * Operation Tests (Interface v1)
// ============================================================================

func TestV1_SelectStar(t *testing.T) {
	RequireInterfaceVersion(t, 1)

	dbClient := DbClientV1(BaseURL)
	ctx := context.Background()

	RunTracked(t, "SelectStar_EmptyTable", func(t *testing.T) {
		_ = SetupTestTableV1(t, dbClient, ctx, "people")
		// No COPY - table is empty

		result := ExecuteSelectStarV1(t, dbClient, ctx, "people")
		rows := ParseQueryResultsV1(result)
		require.Empty(t, rows, "Empty table should return no rows")
	})

	RunTracked(t, "SelectStar_AfterCopy_AllDataPresent", func(t *testing.T) {
		_ = SetupTestTableV1(t, dbClient, ctx, "people")
		LoadTestDataV1(t, dbClient, ctx, "people")

		result := ExecuteSelectStarV1(t, dbClient, ctx, "people")
		rows := ParseQueryResultsV1(result)

		require.Len(t, rows, 5, "Should return all 5 rows from people.csv")
	})

	RunTracked(t, "SelectStar_CorrectColumnCount", func(t *testing.T) {
		_ = SetupTestTableV1(t, dbClient, ctx, "people")
		LoadTestDataV1(t, dbClient, ctx, "people")

		result := ExecuteSelectStarV1(t, dbClient, ctx, "people")
		rows := ParseQueryResultsV1(result)

		require.NotEmpty(t, rows)
		// people has 4 columns: id, name, surname, age
		require.Len(t, rows[0], 4, "Should have 4 columns (id, name, surname, age)")
	})

	RunTracked(t, "SelectStar_NonExistentTable_Fails", func(t *testing.T) {
		t.Log(pit.FormatRequest("POST", "/query", map[string]interface{}{"tableName": "non_existent_table_xyz"}))
		queryId, resp, err := SubmitSelectStarQueryV1(dbClient, ctx, "non_existent_table_xyz")
		t.Log(pit.FormatResponse(resp))

		// May fail at submission (400) or during execution (FAILED)
		if err == nil && resp.StatusCode == http.StatusOK {
			query, waitErr := WaitForQueryCompletionV1(dbClient, ctx, queryId, 10*time.Second)
			require.NoError(t, waitErr)
			require.Equal(t, openapi1.FAILED, query.GetStatus(),
				"SELECT * from non-existent table should fail")
		} else {
			require.Equal(t, http.StatusBadRequest, resp.StatusCode,
				"SELECT * from non-existent table should return 400")
		}
	})

	RunTracked(t, "SelectStar_DataTypes_INT64", func(t *testing.T) {
		_ = SetupTestTableV1(t, dbClient, ctx, "types_test")
		LoadTestDataV1(t, dbClient, ctx, "types_test")

		result := ExecuteSelectStarV1(t, dbClient, ctx, "types_test")
		rows := ParseQueryResultsV1(result)

		require.NotEmpty(t, rows)
		// types_test has: int_col INT64, varchar_col VARCHAR
		// Column order may vary, but at least one column should be int64
		foundInt64 := false
		for _, row := range rows {
			for _, val := range row {
				if _, ok := val.(int64); ok {
					foundInt64 = true
					break
				}
			}
			if foundInt64 {
				break
			}
		}
		require.True(t, foundInt64, "Should have INT64 values in result")
	})

	RunTracked(t, "SelectStar_DataTypes_VARCHAR", func(t *testing.T) {
		_ = SetupTestTableV1(t, dbClient, ctx, "types_test")
		LoadTestDataV1(t, dbClient, ctx, "types_test")

		result := ExecuteSelectStarV1(t, dbClient, ctx, "types_test")
		rows := ParseQueryResultsV1(result)

		require.NotEmpty(t, rows)
		// types_test has: int_col INT64, varchar_col VARCHAR
		// Column order may vary, but at least one column should be string
		foundString := false
		for _, row := range rows {
			for _, val := range row {
				if _, ok := val.(string); ok {
					foundString = true
					break
				}
			}
			if foundString {
				break
			}
		}
		require.True(t, foundString, "Should have VARCHAR values in result")
	})

	RunTracked(t, "SelectStar_QueryStatusTransitions", func(t *testing.T) {
		_ = SetupTestTableV1(t, dbClient, ctx, "people")
		LoadTestDataV1(t, dbClient, ctx, "people")

		t.Log(pit.FormatRequest("POST", "/query", map[string]interface{}{"tableName": "people"}))
		queryId, resp, err := SubmitSelectStarQueryV1(dbClient, ctx, "people")
		t.Log(pit.FormatResponse(resp))
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode)

		// Query should eventually complete
		query, err := WaitForQueryCompletionV1(dbClient, ctx, queryId, 30*time.Second)
		require.NoError(t, err)
		require.Equal(t, openapi1.COMPLETED, query.GetStatus())
	})

	RunTracked(t, "SelectStar_ResultFlush", func(t *testing.T) {
		_ = SetupTestTableV1(t, dbClient, ctx, "people")
		LoadTestDataV1(t, dbClient, ctx, "people")

		t.Log(pit.FormatRequest("POST", "/query", map[string]interface{}{"tableName": "people"}))
		queryId, resp, err := SubmitSelectStarQueryV1(dbClient, ctx, "people")
		t.Log(pit.FormatResponse(resp))
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode)

		query, err := WaitForQueryCompletionV1(dbClient, ctx, queryId, 30*time.Second)
		require.NoError(t, err)
		require.Equal(t, openapi1.COMPLETED, query.GetStatus())

		// Get result with flush
		t.Logf("Sending request:\nGET /result/%s", queryId)
		result, err := GetQueryResultV1(dbClient, ctx, queryId)
		require.NoError(t, err)
		require.NotEmpty(t, result)

		rows := ParseQueryResultsV1(result)
		require.Len(t, rows, 5)
	})
}
