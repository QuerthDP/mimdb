package parser

import (
	"fmt"
	"strconv"
	"strings"

	openapi "github.com/smogork/ISBD-MIMUW/pit/client/openapi2"
)

// Modify in place the ColumnReferenceExpressions in colExpr to fill in missing table names
func diveIntoColumnExpression(colExpr *openapi.ColumnExpression, from []string) error {
	// WHen we find a ColumnReferenceExpression without a table name, try to guess it from FROM clause
	if colExpr.ColumnReferenceExpression != nil {
		if colExpr.ColumnReferenceExpression.TableName == nil || *colExpr.ColumnReferenceExpression.TableName == "" {
			if len(from) > 1 {
				// Ambiguous, cannot guess
				return fmt.Errorf("cannot guess table name for column %s with multiple FROM tables", *colExpr.ColumnReferenceExpression.ColumnName)
			}
			// Try to guess the table name from the FROM clause
			colExpr.ColumnReferenceExpression.TableName = &from[0]
			return nil
		}
	} else if colExpr.ColumnarBinaryOperation != nil { // Dive into both operands
		err_left := diveIntoColumnExpression(colExpr.ColumnarBinaryOperation.LeftOperand, from)
		if err_left != nil {
			return err_left
		}
		err_right := diveIntoColumnExpression(colExpr.ColumnarBinaryOperation.RightOperand, from)
		if err_right != nil {
			return err_right
		}
	} else if colExpr.ColumnarUnaryOperation != nil { // Dive into operand
		return diveIntoColumnExpression(colExpr.ColumnarUnaryOperation.Operand, from)
	} else if colExpr.Function != nil { // Dive into all arguments
		for i := range colExpr.Function.Arguments {
			if err := diveIntoColumnExpression(&colExpr.Function.Arguments[i], from); err != nil {
				return err
			}
		}
	}

	// Literals do not have column references, nothing to do
	return nil
}

func guessTableNames(cols []openapi.ColumnExpression, from []string) ([]openapi.ColumnExpression, error) {
	for _, col := range cols {
		if err := diveIntoColumnExpression(&col, from); err != nil {
			return nil, err
		}
	}
	return cols, nil
}

// buildQuery constructs an openapi.SelectQuery from parsed components
func buildQuery(columns, from, where, orderBy, limit interface{}) (*openapi.SelectQuery, error) {
	guessedColRefences, err := guessTableNames(columns.([]openapi.ColumnExpression), from.([]string))
	if err != nil {
		return nil, err
	}

	query := openapi.NewSelectQuery(guessedColRefences)

	if where != nil {

		expr := where.(*openapi.ColumnExpression)

		err := diveIntoColumnExpression(expr, from.([]string))
		if err != nil {
			return nil, err
		}

		query.WhereClause = expr
	}

	if orderBy != nil {
		query.OrderByClause = orderBy.([]openapi.OrderByExpression)
	}

	if limit != nil {
		query.LimitClause = limit.(*openapi.LimitExpression)
	}

	return query, nil
}

// buildColumnList constructs a slice of ColumnExpression from first and rest
func buildColumnList(first, rest interface{}) []openapi.ColumnExpression {
	cols := []openapi.ColumnExpression{*first.(*openapi.ColumnExpression)}
	if rest != nil {
		for _, r := range rest.([]interface{}) {
			cols = append(cols, *r.(*openapi.ColumnExpression))
		}
	}
	return cols
}

func buildFromClause(first interface{}, rest interface{}) []string {
	fromClauses := []string{first.(string)}
	if rest != nil {
		for _, r := range rest.([]interface{}) {
			fromClauses = append(fromClauses, r.(string))
		}
	}
	return fromClauses
}

// buildOrderByList constructs a slice of OrderByExpression
func buildOrderByList(first, rest interface{}) []openapi.OrderByExpression {
	items := []openapi.OrderByExpression{*first.(*openapi.OrderByExpression)}
	if rest != nil {
		for _, r := range rest.([]interface{}) {
			items = append(items, *r.(*openapi.OrderByExpression))
		}
	}
	return items
}

// buildOrderByItem constructs an OrderByExpression
func buildOrderByItem(idx, dir interface{}) *openapi.OrderByExpression {
	colIdx := int32(idx.(int64))
	ascending := true
	if dir != nil {
		ascending = strings.ToUpper(strings.TrimSpace(dir.(string))) == "ASC"
	}
	return &openapi.OrderByExpression{
		ColumnIndex: &colIdx,
		Ascending:   &ascending,
	}
}

// buildLimitClause constructs a LimitExpression
func buildLimitClause(n interface{}) *openapi.LimitExpression {
	limit := int32(n.(int64))
	return &openapi.LimitExpression{
		Limit: &limit,
	}
}

// buildBinaryChain constructs a left-associative chain of binary operations
func buildBinaryChain(first, rest interface{}, op string) *openapi.ColumnExpression {
	result := first.(*openapi.ColumnExpression)
	if rest == nil {
		return result
	}
	for _, r := range rest.([]interface{}) {
		right := r.(*openapi.ColumnExpression)
		opCopy := op
		result = &openapi.ColumnExpression{
			ColumnarBinaryOperation: &openapi.ColumnarBinaryOperation{
				Operator:     &opCopy,
				LeftOperand:  result,
				RightOperand: right,
			},
		}
	}
	return result
}

// buildComparison constructs a comparison expression or returns left if no operator
func buildComparison(left, op, right interface{}) *openapi.ColumnExpression {
	leftExpr := left.(*openapi.ColumnExpression)
	if op == nil {
		return leftExpr
	}

	opStr := strings.TrimSpace(op.(string))
	opMap := map[string]string{
		"=":  "EQUAL",
		"<>": "NOT_EQUAL",
		"!=": "NOT_EQUAL",
		"<":  "LESS_THAN",
		"<=": "LESS_EQUAL",
		">":  "GREATER_THAN",
		">=": "GREATER_EQUAL",
	}
	mappedOp := opMap[opStr]

	return &openapi.ColumnExpression{
		ColumnarBinaryOperation: &openapi.ColumnarBinaryOperation{
			Operator:     &mappedOp,
			LeftOperand:  leftExpr,
			RightOperand: right.(*openapi.ColumnExpression),
		},
	}
}

// buildAdditiveChain constructs a chain of + and - operations
func buildAdditiveChain(first, rest interface{}) *openapi.ColumnExpression {
	result := first.(*openapi.ColumnExpression)
	if rest == nil {
		return result
	}
	for _, r := range rest.([]interface{}) {
		pair := r.([]interface{})
		opStr := strings.TrimSpace(pair[0].(string))
		right := pair[1].(*openapi.ColumnExpression)

		op := "ADD"
		if opStr == "-" {
			op = "SUBTRACT"
		}

		result = &openapi.ColumnExpression{
			ColumnarBinaryOperation: &openapi.ColumnarBinaryOperation{
				Operator:     &op,
				LeftOperand:  result,
				RightOperand: right,
			},
		}
	}
	return result
}

// buildMultiplicativeChain constructs a chain of * and / operations
func buildMultiplicativeChain(first, rest interface{}) *openapi.ColumnExpression {
	result := first.(*openapi.ColumnExpression)
	if rest == nil {
		return result
	}
	for _, r := range rest.([]interface{}) {
		pair := r.([]interface{})
		opStr := strings.TrimSpace(pair[0].(string))
		right := pair[1].(*openapi.ColumnExpression)

		op := "MULTIPLY"
		if opStr == "/" {
			op = "DIVIDE"
		}

		result = &openapi.ColumnExpression{
			ColumnarBinaryOperation: &openapi.ColumnarBinaryOperation{
				Operator:     &op,
				LeftOperand:  result,
				RightOperand: right,
			},
		}
	}
	return result
}

// buildUnary constructs a unary operation or returns primary if no operator
func buildUnary(op, primary interface{}) *openapi.ColumnExpression {
	primaryExpr := primary.(*openapi.ColumnExpression)
	if op == nil {
		return primaryExpr
	}

	opStr := strings.ToUpper(strings.TrimSpace(op.(string)))
	var mappedOp string
	if opStr == "NOT" {
		mappedOp = "NOT"
	} else if opStr == "-" {
		mappedOp = "MINUS"
	} else {
		return primaryExpr
	}

	return &openapi.ColumnExpression{
		ColumnarUnaryOperation: &openapi.ColumnarUnaryOperation{
			Operator: &mappedOp,
			Operand:  primaryExpr,
		},
	}
}

// buildIntLiteral constructs a Literal with an int64 value
func buildIntLiteral(s string) *openapi.ColumnExpression {
	val, _ := strconv.ParseInt(s, 10, 64)
	litVal := openapi.Int64AsLiteralValue(&val)
	return &openapi.ColumnExpression{
		Literal: &openapi.Literal{Value: &litVal},
	}
}

// buildStringLiteral constructs a Literal with a string value
func buildStringLiteral(content interface{}) *openapi.ColumnExpression {
	s := ""
	if content != nil {
		s = content.(string)
		// Handle escaped single quotes
		s = strings.ReplaceAll(s, "''", "'")
	}
	litVal := openapi.StringAsLiteralValue(&s)
	return &openapi.ColumnExpression{
		Literal: &openapi.Literal{Value: &litVal},
	}
}

// buildBoolLiteral constructs a Literal with a boolean value
func buildBoolLiteral(s string) *openapi.ColumnExpression {
	val := strings.ToUpper(s) == "TRUE"
	litVal := openapi.BoolAsLiteralValue(&val)
	return &openapi.ColumnExpression{
		Literal: &openapi.Literal{Value: &litVal},
	}
}

// buildFunction constructs a Function expression
func buildFunction(name, args interface{}) *openapi.ColumnExpression {
	funcName := strings.ToUpper(name.(string))
	var arguments []openapi.ColumnExpression
	if args != nil {
		arguments = args.([]openapi.ColumnExpression)
	}
	return &openapi.ColumnExpression{
		Function: &openapi.Function{
			FunctionName: &funcName,
			Arguments:    arguments,
		},
	}
}

// buildArgsList constructs a slice of ColumnExpression for function arguments
func buildArgsList(first, rest interface{}) []openapi.ColumnExpression {
	args := []openapi.ColumnExpression{*first.(*openapi.ColumnExpression)}
	if rest != nil {
		for _, r := range rest.([]interface{}) {
			args = append(args, *r.(*openapi.ColumnExpression))
		}
	}
	return args
}

// buildColumnRef constructs a ColumnReferenceExpression
func buildColumnRef(table, column interface{}) *openapi.ColumnExpression {
	colName := column.(string)
	ref := &openapi.ColumnReferenceExpression{
		ColumnName: &colName,
	}
	if table != nil {
		tableName := table.(string)
		ref.TableName = &tableName
	}
	return &openapi.ColumnExpression{
		ColumnReferenceExpression: ref,
	}
}

// parseInt parses a string to int64
func parseInt(s string) int64 {
	val, _ := strconv.ParseInt(s, 10, 64)
	return val
}
