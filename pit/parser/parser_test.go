package parser

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseSimpleSelect(t *testing.T) {
	input := "SELECT c1 FROM t1"
	query, err := ParseSQL(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(query.ColumnClauses) != 1 {
		t.Errorf("expected 1 column clause, got %d", len(query.ColumnClauses))
	}

	col := query.ColumnClauses[0]
	if col.ColumnReferenceExpression == nil {
		t.Error("expected ColumnReferenceExpression")
		return
	}
	if *col.ColumnReferenceExpression.ColumnName != "c1" {
		t.Errorf("expected column name 'c1', got '%s'", *col.ColumnReferenceExpression.ColumnName)
	}
}

func TestParseArithmeticExpression(t *testing.T) {
	// SELECT (c1 + c2) / c3 FROM t1
	input := "SELECT (c1 + c2) / c3 FROM t1"
	query, err := ParseSQL(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(query.ColumnClauses) != 1 {
		t.Errorf("expected 1 column clause, got %d", len(query.ColumnClauses))
		return
	}

	// Should be: DIVIDE(ADD(c1, c2), c3)
	col := query.ColumnClauses[0]
	if col.ColumnarBinaryOperation == nil {
		t.Error("expected ColumnarBinaryOperation at top level")
		return
	}
	if *col.ColumnarBinaryOperation.Operator != "DIVIDE" {
		t.Errorf("expected DIVIDE operator, got %s", *col.ColumnarBinaryOperation.Operator)
	}

	// Left operand should be ADD
	left := col.ColumnarBinaryOperation.LeftOperand
	if left.ColumnarBinaryOperation == nil {
		t.Error("expected ColumnarBinaryOperation for left operand")
		return
	}
	if *left.ColumnarBinaryOperation.Operator != "ADD" {
		t.Errorf("expected ADD operator, got %s", *left.ColumnarBinaryOperation.Operator)
	}
}

func TestParseWhereClause(t *testing.T) {
	input := "SELECT a FROM t1 WHERE b > 10 AND c < 20"
	query, err := ParseSQL(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if query.WhereClause == nil {
		t.Error("expected WHERE clause")
		return
	}

	// Should be AND(GT(b, 10), LT(c, 20))
	if query.WhereClause.ColumnarBinaryOperation == nil {
		t.Error("expected ColumnarBinaryOperation for WHERE clause")
		return
	}
	if *query.WhereClause.ColumnarBinaryOperation.Operator != "AND" {
		t.Errorf("expected AND operator, got %s", *query.WhereClause.ColumnarBinaryOperation.Operator)
	}
}

func TestParseOrderByAndLimit(t *testing.T) {
	input := "SELECT a, b FROM t1 ORDER BY 0 ASC, 1 DESC LIMIT 100"
	query, err := ParseSQL(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(query.OrderByClause) != 2 {
		t.Errorf("expected 2 ORDER BY items, got %d", len(query.OrderByClause))
	}

	// First item: column 0, ascending
	if *query.OrderByClause[0].ColumnIndex != 0 {
		t.Errorf("expected column index 0, got %d", *query.OrderByClause[0].ColumnIndex)
	}
	if !*query.OrderByClause[0].Ascending {
		t.Error("expected ascending for first ORDER BY item")
	}

	// Second item: column 1, descending
	if *query.OrderByClause[1].ColumnIndex != 1 {
		t.Errorf("expected column index 1, got %d", *query.OrderByClause[1].ColumnIndex)
	}
	if *query.OrderByClause[1].Ascending {
		t.Error("expected descending for second ORDER BY item")
	}

	// LIMIT
	if query.LimitClause == nil {
		t.Error("expected LIMIT clause")
		return
	}
	if *query.LimitClause.Limit != 100 {
		t.Errorf("expected limit 100, got %d", *query.LimitClause.Limit)
	}
}

func TestParseFunctions(t *testing.T) {
	input := "SELECT UPPER(name), STRLEN(description), CONCAT(a, b), REPLACE(c, 'old', 'new') FROM t1"
	query, err := ParseSQL(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(query.ColumnClauses) != 4 {
		t.Errorf("expected 4 column clauses, got %d", len(query.ColumnClauses))
		return
	}

	// First: UPPER(name)
	if query.ColumnClauses[0].Function == nil {
		t.Error("expected Function for first column")
	} else if *query.ColumnClauses[0].Function.FunctionName != "UPPER" {
		t.Errorf("expected UPPER function, got %s", *query.ColumnClauses[0].Function.FunctionName)
	}

	// Second: STRLEN(description)
	if query.ColumnClauses[1].Function == nil {
		t.Error("expected Function for second column")
	} else if *query.ColumnClauses[1].Function.FunctionName != "STRLEN" {
		t.Errorf("expected STRLEN function, got %s", *query.ColumnClauses[1].Function.FunctionName)
	}

	// Third: CONCAT(a, b)
	if query.ColumnClauses[2].Function == nil {
		t.Error("expected Function for third column")
	} else {
		f := query.ColumnClauses[2].Function
		if *f.FunctionName != "CONCAT" {
			t.Errorf("expected CONCAT function, got %s", *f.FunctionName)
		}
		if len(f.Arguments) != 2 {
			t.Errorf("expected 2 arguments for CONCAT, got %d", len(f.Arguments))
		}
	}

	// Fourth: REPLACE(c, 'old', 'new')
	if query.ColumnClauses[3].Function == nil {
		t.Error("expected Function for fourth column")
	} else {
		f := query.ColumnClauses[3].Function
		if *f.FunctionName != "REPLACE" {
			t.Errorf("expected REPLACE function, got %s", *f.FunctionName)
		}
		if len(f.Arguments) != 3 {
			t.Errorf("expected 3 arguments for REPLACE, got %d", len(f.Arguments))
		}
	}
}

func TestParseLiterals(t *testing.T) {
	input := "SELECT 42, 'hello', TRUE, FALSE FROM t1"
	query, err := ParseSQL(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(query.ColumnClauses) != 4 {
		t.Errorf("expected 4 column clauses, got %d", len(query.ColumnClauses))
		return
	}

	// Integer literal
	if query.ColumnClauses[0].Literal == nil {
		t.Error("expected Literal for first column")
	} else if *query.ColumnClauses[0].Literal.Value.Int64 != 42 {
		t.Errorf("expected integer 42, got %v", query.ColumnClauses[0].Literal.Value)
	}

	// String literal
	if query.ColumnClauses[1].Literal == nil {
		t.Error("expected Literal for second column")
	} else if *query.ColumnClauses[1].Literal.Value.String != "hello" {
		t.Errorf("expected string 'hello', got %v", query.ColumnClauses[1].Literal.Value)
	}

	// TRUE
	if query.ColumnClauses[2].Literal == nil {
		t.Error("expected Literal for third column")
	} else if !*query.ColumnClauses[2].Literal.Value.Bool {
		t.Error("expected TRUE")
	}

	// FALSE
	if query.ColumnClauses[3].Literal == nil {
		t.Error("expected Literal for fourth column")
	} else if *query.ColumnClauses[3].Literal.Value.Bool {
		t.Error("expected FALSE")
	}
}

func TestParseTableQualifiedColumn(t *testing.T) {
	input := "SELECT t1.c1, t2.c2 FROM t1"
	query, err := ParseSQL(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(query.ColumnClauses) != 2 {
		t.Errorf("expected 2 column clauses, got %d", len(query.ColumnClauses))
		return
	}

	// First: t1.c1
	col1 := query.ColumnClauses[0].ColumnReferenceExpression
	if col1 == nil {
		t.Error("expected ColumnReferenceExpression for first column")
		return
	}
	if *col1.TableName != "t1" {
		t.Errorf("expected table name 't1', got '%s'", *col1.TableName)
	}
	if *col1.ColumnName != "c1" {
		t.Errorf("expected column name 'c1', got '%s'", *col1.ColumnName)
	}
}

func TestParseUnaryOperators(t *testing.T) {
	input := "SELECT -a, NOT b FROM t1"
	query, err := ParseSQL(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(query.ColumnClauses) != 2 {
		t.Errorf("expected 2 column clauses, got %d", len(query.ColumnClauses))
		return
	}

	// First: -a (MINUS)
	if query.ColumnClauses[0].ColumnarUnaryOperation == nil {
		t.Error("expected ColumnarUnaryOperation for first column")
	} else if *query.ColumnClauses[0].ColumnarUnaryOperation.Operator != "MINUS" {
		t.Errorf("expected MINUS operator, got %s", *query.ColumnClauses[0].ColumnarUnaryOperation.Operator)
	}

	// Second: NOT b
	if query.ColumnClauses[1].ColumnarUnaryOperation == nil {
		t.Error("expected ColumnarUnaryOperation for second column")
	} else if *query.ColumnClauses[1].ColumnarUnaryOperation.Operator != "NOT" {
		t.Errorf("expected NOT operator, got %s", *query.ColumnClauses[1].ColumnarUnaryOperation.Operator)
	}
}

func TestParseComplexExpression(t *testing.T) {
	// (a + b) * c - d / e
	input := "SELECT (a + b) * c - d / e FROM t1"
	query, err := ParseSQL(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Just verify it parses without error and produces valid JSON
	jsonBytes, err := json.Marshal(query)
	if err != nil {
		t.Fatalf("failed to marshal to JSON: %v", err)
	}

	t.Logf("Parsed query JSON: %s", string(jsonBytes))
}

func TestJSONOutput(t *testing.T) {
	input := "SELECT (c1 + c2) / c3 FROM t1"
	query, err := ParseSQL(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	jsonBytes, err := json.MarshalIndent(query, "", "  ")
	if err != nil {
		t.Fatalf("failed to marshal to JSON: %v", err)
	}

	t.Logf("JSON output:\n%s", string(jsonBytes))

	// Verify JSON can be unmarshaled back
	var result map[string]interface{}
	if err := json.Unmarshal(jsonBytes, &result); err != nil {
		t.Fatalf("failed to unmarshal JSON: %v", err)
	}

	// Check that columnClauses exists
	if _, ok := result["columnClauses"]; !ok {
		t.Error("expected 'columnClauses' in JSON output")
	}
}

func TestMultipleFromClauses(t *testing.T) {
	input := "SELECT t1.c1, t2.c2 FROM t1, t2"
	query, err := ParseSQL(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	require.Equal(t, "t1", *query.ColumnClauses[0].ColumnReferenceExpression.TableName)
	require.Equal(t, "t2", *query.ColumnClauses[1].ColumnReferenceExpression.TableName)
	require.Equal(t, "c1", *query.ColumnClauses[0].ColumnReferenceExpression.ColumnName)
	require.Equal(t, "c2", *query.ColumnClauses[1].ColumnReferenceExpression.ColumnName)
}

func TestMultipleFromClausesDisambigous(t *testing.T) {
	input := "SELECT t1.c1, c2 FROM t1, t2"
	query, err := ParseSQL(input)
	require.Nil(t, query)
	require.ErrorContains(t, err, "cannot guess table name for column c2 with multiple FROM tables")
}

func TestTableGuessing(t *testing.T) {
	input := "SELECT c1, c2 FROM t1"
	query, err := ParseSQL(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	require.Equal(t, "t1", *query.ColumnClauses[0].ColumnReferenceExpression.TableName)
	require.Equal(t, "t1", *query.ColumnClauses[1].ColumnReferenceExpression.TableName)
	require.Equal(t, "c1", *query.ColumnClauses[0].ColumnReferenceExpression.ColumnName)
	require.Equal(t, "c2", *query.ColumnClauses[1].ColumnReferenceExpression.ColumnName)
}

func TestTableGuessingInOperands(t *testing.T) {
	input := "SELECT c1+c2 FROM t1"
	query, err := ParseSQL(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	require.Equal(t, "t1", *query.ColumnClauses[0].ColumnarBinaryOperation.LeftOperand.ColumnReferenceExpression.TableName)
	require.Equal(t, "t1", *query.ColumnClauses[0].ColumnarBinaryOperation.RightOperand.ColumnReferenceExpression.TableName)
	require.Equal(t, "c1", *query.ColumnClauses[0].ColumnarBinaryOperation.LeftOperand.ColumnReferenceExpression.ColumnName)
	require.Equal(t, "c2", *query.ColumnClauses[0].ColumnarBinaryOperation.RightOperand.ColumnReferenceExpression.ColumnName)
}

func TestTableGuessingInUnaryOperand(t *testing.T) {
	input := "SELECT -c1 FROM t1"
	query, err := ParseSQL(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	require.Equal(t, "t1", *query.ColumnClauses[0].ColumnarUnaryOperation.Operand.ColumnReferenceExpression.TableName)
	require.Equal(t, "c1", *query.ColumnClauses[0].ColumnarUnaryOperation.Operand.ColumnReferenceExpression.ColumnName)
}

func TestTableGuessingInFunction(t *testing.T) {
	input := "SELECT STRLEN(c1) FROM t1"
	query, err := ParseSQL(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	require.Equal(t, "t1", *query.ColumnClauses[0].Function.Arguments[0].ColumnReferenceExpression.TableName)
	require.Equal(t, "c1", *query.ColumnClauses[0].Function.Arguments[0].ColumnReferenceExpression.ColumnName)
}

func TestMissingFrom(t *testing.T) {
	input := "SELECT c1"
	query, err := ParseSQL(input)
	require.Nil(t, query)
	require.ErrorContains(t, err, "FROM clause is required")
}

func TestWhereClause(t *testing.T) {
	input := "SELECT c1 FROM t1 WHERE c1 = 10"
	query, err := ParseSQL(input)
	require.NoError(t, err)

	require.Equal(t, "c1", *query.WhereClause.ColumnarBinaryOperation.LeftOperand.ColumnReferenceExpression.ColumnName)
	require.Equal(t, "t1", *query.WhereClause.ColumnarBinaryOperation.LeftOperand.ColumnReferenceExpression.TableName)
	require.Equal(t, int64(10), *query.WhereClause.ColumnarBinaryOperation.RightOperand.Literal.Value.Int64)
	require.Equal(t, "EQUAL", *query.WhereClause.ColumnarBinaryOperation.Operator)
}

func TestWhereWithOrder(t *testing.T) {
	input := "SELECT c1 FROM t1 WHERE c1 = 10 ORDER BY 0 ASC"
	query, err := ParseSQL(input)
	require.NoError(t, err)

	require.Equal(t, "c1", *query.WhereClause.ColumnarBinaryOperation.LeftOperand.ColumnReferenceExpression.ColumnName)
	require.Equal(t, "t1", *query.WhereClause.ColumnarBinaryOperation.LeftOperand.ColumnReferenceExpression.TableName)
	require.Equal(t, int64(10), *query.WhereClause.ColumnarBinaryOperation.RightOperand.Literal.Value.Int64)
	require.Equal(t, "EQUAL", *query.WhereClause.ColumnarBinaryOperation.Operator)

	require.Equal(t, int32(0), *query.OrderByClause[0].ColumnIndex)
	require.True(t, *query.OrderByClause[0].Ascending)
}
