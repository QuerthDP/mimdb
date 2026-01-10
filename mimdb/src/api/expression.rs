/*
 * Copyright (c) 2026-present Dawid Pawlik
 *
 * For educational use only by employees and students of MIMUW.
 * See LICENSE file for details.
 */

//! # Expression Evaluator
//!
//! This module handles evaluation of column expressions in SELECT queries.
//! It supports literals, column references, functions, and operators.
//! Expressions are evaluated column-wise (all rows at once) for efficiency.

use crate::ColumnData;
use crate::ColumnType;
use crate::api::models::BinaryOperator;
use crate::api::models::ColumnExpression;
use crate::api::models::ColumnReferenceExpression;
use crate::api::models::ColumnarBinaryOperation;
use crate::api::models::ColumnarUnaryOperation;
use crate::api::models::Function;
use crate::api::models::FunctionName;
use crate::api::models::Literal;
use crate::api::models::LiteralValue;
use crate::api::models::UnaryOperator;
use anyhow::Result;
use std::collections::HashMap;

/// Context for expression evaluation - provides access to column data
pub struct EvaluationContext<'a> {
    /// Column data indexed by (table_name, column_name)
    pub columns: HashMap<(String, String), &'a ColumnData>,
    /// Total number of rows in the data
    pub row_count: usize,
}

impl<'a> EvaluationContext<'a> {
    pub fn new(columns: HashMap<(String, String), &'a ColumnData>, row_count: usize) -> Self {
        Self { columns, row_count }
    }

    pub fn get_column_data(&self, table_name: &str, column_name: &str) -> Result<&'a ColumnData> {
        let key = (table_name.to_string(), column_name.to_string());
        self.columns
            .get(&key)
            .copied()
            .ok_or_else(|| anyhow::anyhow!("Column '{}.{}' not found", table_name, column_name))
    }
}

/// Expression evaluator - evaluates expressions column-wise (all rows at once)
pub struct ExpressionEvaluator;

impl ExpressionEvaluator {
    /// Evaluate an expression
    pub fn evaluate(expr: &ColumnExpression, ctx: &EvaluationContext) -> Result<ColumnData> {
        match expr {
            ColumnExpression::Literal(lit) => Self::evaluate_literal(lit, ctx.row_count),
            ColumnExpression::ColumnReference(col_ref) => {
                Self::evaluate_column_reference(col_ref, ctx)
            }
            ColumnExpression::Function(func) => Self::evaluate_function(func, ctx),
            ColumnExpression::BinaryOperation(bin_op) => Self::evaluate_binary_op(bin_op, ctx),
            ColumnExpression::UnaryOperation(unary_op) => Self::evaluate_unary_op(unary_op, ctx),
        }
    }

    /// Evaluate a literal value
    fn evaluate_literal(lit: &Literal, row_count: usize) -> Result<ColumnData> {
        match &lit.value {
            LiteralValue::Int64(v) => Ok(ColumnData::Int64(vec![*v; row_count])),
            LiteralValue::Varchar(v) => Ok(ColumnData::Varchar(vec![v.clone(); row_count])),
            LiteralValue::Bool(v) => Ok(ColumnData::Bool(vec![*v; row_count])),
        }
    }

    /// Evaluate a column reference
    fn evaluate_column_reference(
        col_ref: &ColumnReferenceExpression,
        ctx: &EvaluationContext,
    ) -> Result<ColumnData> {
        let data = ctx.get_column_data(&col_ref.table_name, &col_ref.column_name)?;
        Ok(data.clone())
    }

    /// Evaluate a function call
    fn evaluate_function(func: &Function, ctx: &EvaluationContext) -> Result<ColumnData> {
        match func.function_name {
            FunctionName::Strlen => {
                if func.arguments.len() != 1 {
                    anyhow::bail!("STRLEN expects exactly 1 argument");
                }
                let arg = Self::evaluate(&func.arguments[0], ctx)?;
                match arg {
                    ColumnData::Varchar(vec) => Ok(ColumnData::Int64(
                        vec.iter().map(|s| s.len() as i64).collect(),
                    )),
                    _ => anyhow::bail!("STRLEN expects a VARCHAR argument"),
                }
            }
            FunctionName::Concat => {
                if func.arguments.len() != 2 {
                    anyhow::bail!("CONCAT expects exactly 2 arguments");
                }
                let left = Self::evaluate(&func.arguments[0], ctx)?;
                let right = Self::evaluate(&func.arguments[1], ctx)?;
                match (left, right) {
                    (ColumnData::Varchar(v1), ColumnData::Varchar(v2)) => Ok(ColumnData::Varchar(
                        v1.iter()
                            .zip(v2.iter())
                            .map(|(s1, s2)| format!("{}{}", s1, s2))
                            .collect(),
                    )),
                    _ => anyhow::bail!("CONCAT expects VARCHAR arguments"),
                }
            }
            FunctionName::Upper => {
                if func.arguments.len() != 1 {
                    anyhow::bail!("UPPER expects exactly 1 argument");
                }
                let arg = Self::evaluate(&func.arguments[0], ctx)?;
                match arg {
                    ColumnData::Varchar(vec) => Ok(ColumnData::Varchar(
                        vec.iter().map(|s| s.to_ascii_uppercase()).collect(),
                    )),
                    _ => anyhow::bail!("UPPER expects a VARCHAR argument"),
                }
            }
            FunctionName::Lower => {
                if func.arguments.len() != 1 {
                    anyhow::bail!("LOWER expects exactly 1 argument");
                }
                let arg = Self::evaluate(&func.arguments[0], ctx)?;
                match arg {
                    ColumnData::Varchar(vec) => Ok(ColumnData::Varchar(
                        vec.iter().map(|s| s.to_ascii_lowercase()).collect(),
                    )),
                    _ => anyhow::bail!("LOWER expects a VARCHAR argument"),
                }
            }
        }
    }

    /// Evaluate a binary operation
    fn evaluate_binary_op(
        op: &ColumnarBinaryOperation,
        ctx: &EvaluationContext,
    ) -> Result<ColumnData> {
        let left = Self::evaluate(&op.left_operand, ctx)?;
        let right = Self::evaluate(&op.right_operand, ctx)?;

        match op.operator {
            BinaryOperator::Add => Self::binary_arithmetic(left, right, |a, b| a + b, "ADD"),
            BinaryOperator::Subtract => {
                Self::binary_arithmetic(left, right, |a, b| a - b, "SUBTRACT")
            }
            BinaryOperator::Multiply => {
                Self::binary_arithmetic(left, right, |a, b| a * b, "MULTIPLY")
            }
            BinaryOperator::Divide => {
                // Check for division by zero
                if let ColumnData::Int64(ref r) = right
                    && r.contains(&0)
                {
                    anyhow::bail!("Division by zero");
                }
                Self::binary_arithmetic(left, right, |a, b| a / b, "DIVIDE")
            }
            BinaryOperator::And => Self::binary_logical(left, right, |a, b| a && b, "AND"),
            BinaryOperator::Or => Self::binary_logical(left, right, |a, b| a || b, "OR"),
            BinaryOperator::Equal => Self::compare_equal(left, right, true),
            BinaryOperator::NotEqual => Self::compare_equal(left, right, false),
            BinaryOperator::LessThan => Self::compare_order(left, right, |ord| ord.is_lt()),
            BinaryOperator::LessEqual => Self::compare_order(left, right, |ord| ord.is_le()),
            BinaryOperator::GreaterThan => Self::compare_order(left, right, |ord| ord.is_gt()),
            BinaryOperator::GreaterEqual => Self::compare_order(left, right, |ord| ord.is_ge()),
        }
    }

    /// Helper for arithmetic operations on columns
    fn binary_arithmetic<F>(
        left: ColumnData,
        right: ColumnData,
        f: F,
        op_name: &str,
    ) -> Result<ColumnData>
    where
        F: Fn(i64, i64) -> i64,
    {
        match (left, right) {
            (ColumnData::Int64(l), ColumnData::Int64(r)) => Ok(ColumnData::Int64(
                l.iter().zip(r.iter()).map(|(&a, &b)| f(a, b)).collect(),
            )),
            _ => anyhow::bail!("{} expects INT64 operands", op_name),
        }
    }

    /// Helper for logical operations on columns
    fn binary_logical<F>(
        left: ColumnData,
        right: ColumnData,
        f: F,
        op_name: &str,
    ) -> Result<ColumnData>
    where
        F: Fn(bool, bool) -> bool,
    {
        match (left, right) {
            (ColumnData::Bool(l), ColumnData::Bool(r)) => Ok(ColumnData::Bool(
                l.iter().zip(r.iter()).map(|(&a, &b)| f(a, b)).collect(),
            )),
            _ => anyhow::bail!("{} expects BOOL operands", op_name),
        }
    }

    /// Helper for equality comparison on columns
    fn compare_equal(left: ColumnData, right: ColumnData, is_equal: bool) -> Result<ColumnData> {
        let check = if is_equal { |a: bool| a } else { |a: bool| !a };
        let results = match (&left, &right) {
            (ColumnData::Int64(l), ColumnData::Int64(r)) => {
                l.iter().zip(r.iter()).map(|(a, b)| check(a == b)).collect()
            }
            (ColumnData::Varchar(l), ColumnData::Varchar(r)) => {
                l.iter().zip(r.iter()).map(|(a, b)| check(a == b)).collect()
            }
            (ColumnData::Bool(l), ColumnData::Bool(r)) => {
                l.iter().zip(r.iter()).map(|(a, b)| check(a == b)).collect()
            }
            _ => anyhow::bail!(
                "Cannot compare values of different types: {:?} vs {:?}",
                left.column_type(),
                right.column_type()
            ),
        };
        Ok(ColumnData::Bool(results))
    }

    /// Helper for ordering comparison on columns
    fn compare_order<F>(left: ColumnData, right: ColumnData, check: F) -> Result<ColumnData>
    where
        F: Fn(std::cmp::Ordering) -> bool,
    {
        let results = match (&left, &right) {
            (ColumnData::Int64(l), ColumnData::Int64(r)) => l
                .iter()
                .zip(r.iter())
                .map(|(a, b)| check(a.cmp(b)))
                .collect(),
            (ColumnData::Varchar(l), ColumnData::Varchar(r)) => l
                .iter()
                .zip(r.iter())
                .map(|(a, b)| check(a.cmp(b)))
                .collect(),
            (ColumnData::Bool(l), ColumnData::Bool(r)) => l
                .iter()
                .zip(r.iter())
                .map(|(a, b)| check(a.cmp(b)))
                .collect(),
            _ => anyhow::bail!(
                "Cannot compare values of different types: {:?} vs {:?}",
                left.column_type(),
                right.column_type()
            ),
        };
        Ok(ColumnData::Bool(results))
    }

    /// Evaluate a unary operation
    fn evaluate_unary_op(
        op: &ColumnarUnaryOperation,
        ctx: &EvaluationContext,
    ) -> Result<ColumnData> {
        let operand = Self::evaluate(&op.operand, ctx)?;

        match op.operator {
            UnaryOperator::Not => match operand {
                ColumnData::Bool(vec) => Ok(ColumnData::Bool(vec.iter().map(|&b| !b).collect())),
                _ => anyhow::bail!("NOT expects a BOOL operand"),
            },
            UnaryOperator::Minus => match operand {
                ColumnData::Int64(vec) => Ok(ColumnData::Int64(vec.iter().map(|&v| -v).collect())),
                _ => anyhow::bail!("MINUS expects an INT64 operand"),
            },
        }
    }

    /// Infer the result type of an expression without evaluating it
    pub fn infer_type(
        expr: &ColumnExpression,
        column_types: &HashMap<(String, String), ColumnType>,
    ) -> Result<ColumnType> {
        match expr {
            ColumnExpression::Literal(lit) => match &lit.value {
                LiteralValue::Int64(_) => Ok(ColumnType::Int64),
                LiteralValue::Varchar(_) => Ok(ColumnType::Varchar),
                LiteralValue::Bool(_) => Ok(ColumnType::Bool),
            },
            ColumnExpression::ColumnReference(col_ref) => {
                let key = (col_ref.table_name.clone(), col_ref.column_name.clone());
                column_types.get(&key).cloned().ok_or_else(|| {
                    anyhow::anyhow!(
                        "Column '{}.{}' not found",
                        col_ref.table_name,
                        col_ref.column_name
                    )
                })
            }
            ColumnExpression::Function(func) => match func.function_name {
                FunctionName::Strlen => Ok(ColumnType::Int64),
                FunctionName::Concat | FunctionName::Upper | FunctionName::Lower => {
                    Ok(ColumnType::Varchar)
                }
            },
            ColumnExpression::BinaryOperation(bin_op) => match bin_op.operator {
                BinaryOperator::Add
                | BinaryOperator::Subtract
                | BinaryOperator::Multiply
                | BinaryOperator::Divide => Ok(ColumnType::Int64),
                BinaryOperator::And
                | BinaryOperator::Or
                | BinaryOperator::Equal
                | BinaryOperator::NotEqual
                | BinaryOperator::LessThan
                | BinaryOperator::LessEqual
                | BinaryOperator::GreaterThan
                | BinaryOperator::GreaterEqual => Ok(ColumnType::Bool),
            },
            ColumnExpression::UnaryOperation(unary_op) => match unary_op.operator {
                UnaryOperator::Not => Ok(ColumnType::Bool),
                UnaryOperator::Minus => Ok(ColumnType::Int64),
            },
        }
    }
}

/// Collect all table names referenced in an expression
pub fn collect_table_references(expr: &ColumnExpression) -> Vec<String> {
    let mut tables = Vec::new();
    collect_table_references_recursive(expr, &mut tables);
    tables.sort();
    tables.dedup();
    tables
}

fn collect_table_references_recursive(expr: &ColumnExpression, tables: &mut Vec<String>) {
    match expr {
        ColumnExpression::ColumnReference(col_ref) => {
            tables.push(col_ref.table_name.clone());
        }
        ColumnExpression::Function(func) => {
            for arg in &func.arguments {
                collect_table_references_recursive(arg, tables);
            }
        }
        ColumnExpression::BinaryOperation(bin_op) => {
            collect_table_references_recursive(&bin_op.left_operand, tables);
            collect_table_references_recursive(&bin_op.right_operand, tables);
        }
        ColumnExpression::UnaryOperation(unary_op) => {
            collect_table_references_recursive(&unary_op.operand, tables);
        }
        ColumnExpression::Literal(_) => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_literal_int(v: i64) -> ColumnExpression {
        ColumnExpression::Literal(Literal {
            value: LiteralValue::Int64(v),
        })
    }

    fn make_literal_str(v: &str) -> ColumnExpression {
        ColumnExpression::Literal(Literal {
            value: LiteralValue::Varchar(v.to_string()),
        })
    }

    fn make_literal_bool(v: bool) -> ColumnExpression {
        ColumnExpression::Literal(Literal {
            value: LiteralValue::Bool(v),
        })
    }

    #[test]
    fn test_literal_evaluation() {
        let ctx = EvaluationContext::new(HashMap::new(), 3);

        let int_lit = make_literal_int(42);
        match ExpressionEvaluator::evaluate(&int_lit, &ctx).unwrap() {
            ColumnData::Int64(v) => assert_eq!(v, vec![42, 42, 42]),
            _ => panic!("Expected Int64"),
        }

        let str_lit = make_literal_str("hello");
        match ExpressionEvaluator::evaluate(&str_lit, &ctx).unwrap() {
            ColumnData::Varchar(v) => assert_eq!(v, vec!["hello", "hello", "hello"]),
            _ => panic!("Expected Varchar"),
        }

        let bool_lit = make_literal_bool(true);
        match ExpressionEvaluator::evaluate(&bool_lit, &ctx).unwrap() {
            ColumnData::Bool(v) => assert_eq!(v, vec![true, true, true]),
            _ => panic!("Expected Bool"),
        }
    }

    #[test]
    fn test_arithmetic_operations() {
        let ctx = EvaluationContext::new(HashMap::new(), 1);

        // 10 + 5 = 15
        let add = ColumnExpression::BinaryOperation(ColumnarBinaryOperation {
            operator: BinaryOperator::Add,
            left_operand: Box::new(make_literal_int(10)),
            right_operand: Box::new(make_literal_int(5)),
        });
        match ExpressionEvaluator::evaluate(&add, &ctx).unwrap() {
            ColumnData::Int64(v) => assert_eq!(v[0], 15),
            _ => panic!("Expected Int64"),
        }

        // 10 - 3 = 7
        let sub = ColumnExpression::BinaryOperation(ColumnarBinaryOperation {
            operator: BinaryOperator::Subtract,
            left_operand: Box::new(make_literal_int(10)),
            right_operand: Box::new(make_literal_int(3)),
        });
        match ExpressionEvaluator::evaluate(&sub, &ctx).unwrap() {
            ColumnData::Int64(v) => assert_eq!(v[0], 7),
            _ => panic!("Expected Int64"),
        }

        // 6 * 7 = 42
        let mul = ColumnExpression::BinaryOperation(ColumnarBinaryOperation {
            operator: BinaryOperator::Multiply,
            left_operand: Box::new(make_literal_int(6)),
            right_operand: Box::new(make_literal_int(7)),
        });
        match ExpressionEvaluator::evaluate(&mul, &ctx).unwrap() {
            ColumnData::Int64(v) => assert_eq!(v[0], 42),
            _ => panic!("Expected Int64"),
        }

        // 20 / 4 = 5
        let div = ColumnExpression::BinaryOperation(ColumnarBinaryOperation {
            operator: BinaryOperator::Divide,
            left_operand: Box::new(make_literal_int(20)),
            right_operand: Box::new(make_literal_int(4)),
        });
        match ExpressionEvaluator::evaluate(&div, &ctx).unwrap() {
            ColumnData::Int64(v) => assert_eq!(v[0], 5),
            _ => panic!("Expected Int64"),
        }
    }

    #[test]
    fn test_division_by_zero() {
        let ctx = EvaluationContext::new(HashMap::new(), 1);
        let div = ColumnExpression::BinaryOperation(ColumnarBinaryOperation {
            operator: BinaryOperator::Divide,
            left_operand: Box::new(make_literal_int(10)),
            right_operand: Box::new(make_literal_int(0)),
        });
        assert!(ExpressionEvaluator::evaluate(&div, &ctx).is_err());
    }

    #[test]
    fn test_logical_operations() {
        let ctx = EvaluationContext::new(HashMap::new(), 1);

        // true AND false = false
        let and_op = ColumnExpression::BinaryOperation(ColumnarBinaryOperation {
            operator: BinaryOperator::And,
            left_operand: Box::new(make_literal_bool(true)),
            right_operand: Box::new(make_literal_bool(false)),
        });
        match ExpressionEvaluator::evaluate(&and_op, &ctx).unwrap() {
            ColumnData::Bool(v) => assert!(!v[0]),
            _ => panic!("Expected Bool"),
        }

        // true OR false = true
        let or_op = ColumnExpression::BinaryOperation(ColumnarBinaryOperation {
            operator: BinaryOperator::Or,
            left_operand: Box::new(make_literal_bool(true)),
            right_operand: Box::new(make_literal_bool(false)),
        });
        match ExpressionEvaluator::evaluate(&or_op, &ctx).unwrap() {
            ColumnData::Bool(v) => assert!(v[0]),
            _ => panic!("Expected Bool"),
        }

        // NOT true = false
        let not_op = ColumnExpression::UnaryOperation(ColumnarUnaryOperation {
            operator: UnaryOperator::Not,
            operand: Box::new(make_literal_bool(true)),
        });
        match ExpressionEvaluator::evaluate(&not_op, &ctx).unwrap() {
            ColumnData::Bool(v) => assert!(!v[0]),
            _ => panic!("Expected Bool"),
        }
    }

    #[test]
    fn test_comparison_operations() {
        let ctx = EvaluationContext::new(HashMap::new(), 1);

        // 5 = 5
        let eq = ColumnExpression::BinaryOperation(ColumnarBinaryOperation {
            operator: BinaryOperator::Equal,
            left_operand: Box::new(make_literal_int(5)),
            right_operand: Box::new(make_literal_int(5)),
        });
        match ExpressionEvaluator::evaluate(&eq, &ctx).unwrap() {
            ColumnData::Bool(v) => assert!(v[0]),
            _ => panic!("Expected Bool"),
        }

        // 5 != 3
        let neq = ColumnExpression::BinaryOperation(ColumnarBinaryOperation {
            operator: BinaryOperator::NotEqual,
            left_operand: Box::new(make_literal_int(5)),
            right_operand: Box::new(make_literal_int(3)),
        });
        match ExpressionEvaluator::evaluate(&neq, &ctx).unwrap() {
            ColumnData::Bool(v) => assert!(v[0]),
            _ => panic!("Expected Bool"),
        }

        // 3 < 5
        let lt = ColumnExpression::BinaryOperation(ColumnarBinaryOperation {
            operator: BinaryOperator::LessThan,
            left_operand: Box::new(make_literal_int(3)),
            right_operand: Box::new(make_literal_int(5)),
        });
        match ExpressionEvaluator::evaluate(&lt, &ctx).unwrap() {
            ColumnData::Bool(v) => assert!(v[0]),
            _ => panic!("Expected Bool"),
        }

        // "abc" < "abd" (lexicographic)
        let str_lt = ColumnExpression::BinaryOperation(ColumnarBinaryOperation {
            operator: BinaryOperator::LessThan,
            left_operand: Box::new(make_literal_str("abc")),
            right_operand: Box::new(make_literal_str("abd")),
        });
        match ExpressionEvaluator::evaluate(&str_lt, &ctx).unwrap() {
            ColumnData::Bool(v) => assert!(v[0]),
            _ => panic!("Expected Bool"),
        }
    }

    #[test]
    fn test_string_functions() {
        let ctx = EvaluationContext::new(HashMap::new(), 1);

        // STRLEN("hello") = 5
        let strlen = ColumnExpression::Function(Function {
            function_name: FunctionName::Strlen,
            arguments: vec![make_literal_str("hello")],
        });
        match ExpressionEvaluator::evaluate(&strlen, &ctx).unwrap() {
            ColumnData::Int64(v) => assert_eq!(v[0], 5),
            _ => panic!("Expected Int64"),
        }

        // CONCAT("hello", " world") = "hello world"
        let concat = ColumnExpression::Function(Function {
            function_name: FunctionName::Concat,
            arguments: vec![make_literal_str("hello"), make_literal_str(" world")],
        });
        match ExpressionEvaluator::evaluate(&concat, &ctx).unwrap() {
            ColumnData::Varchar(v) => assert_eq!(v[0], "hello world"),
            _ => panic!("Expected Varchar"),
        }

        // UPPER("hello") = "HELLO"
        let upper = ColumnExpression::Function(Function {
            function_name: FunctionName::Upper,
            arguments: vec![make_literal_str("hello")],
        });
        match ExpressionEvaluator::evaluate(&upper, &ctx).unwrap() {
            ColumnData::Varchar(v) => assert_eq!(v[0], "HELLO"),
            _ => panic!("Expected Varchar"),
        }

        // LOWER("HELLO") = "hello"
        let lower = ColumnExpression::Function(Function {
            function_name: FunctionName::Lower,
            arguments: vec![make_literal_str("HELLO")],
        });
        match ExpressionEvaluator::evaluate(&lower, &ctx).unwrap() {
            ColumnData::Varchar(v) => assert_eq!(v[0], "hello"),
            _ => panic!("Expected Varchar"),
        }
    }

    #[test]
    fn test_unary_minus() {
        let ctx = EvaluationContext::new(HashMap::new(), 1);

        // -42 = -42
        let minus = ColumnExpression::UnaryOperation(ColumnarUnaryOperation {
            operator: UnaryOperator::Minus,
            operand: Box::new(make_literal_int(42)),
        });
        match ExpressionEvaluator::evaluate(&minus, &ctx).unwrap() {
            ColumnData::Int64(v) => assert_eq!(v[0], -42),
            _ => panic!("Expected Int64"),
        }
    }

    #[test]
    fn test_column_reference() {
        let data = ColumnData::Int64(vec![10, 20, 30]);
        let mut columns = HashMap::new();
        columns.insert(("users".to_string(), "age".to_string()), &data);
        let ctx = EvaluationContext::new(columns, 3);

        let col_ref = ColumnExpression::ColumnReference(ColumnReferenceExpression {
            table_name: "users".to_string(),
            column_name: "age".to_string(),
        });

        match ExpressionEvaluator::evaluate(&col_ref, &ctx).unwrap() {
            ColumnData::Int64(v) => assert_eq!(v, vec![10, 20, 30]),
            _ => panic!("Expected Int64"),
        }
    }

    #[test]
    fn test_complex_expression() {
        let ctx = EvaluationContext::new(HashMap::new(), 1);

        // STRLEN(CONCAT("hello", " world")) = 11
        let expr = ColumnExpression::Function(Function {
            function_name: FunctionName::Strlen,
            arguments: vec![ColumnExpression::Function(Function {
                function_name: FunctionName::Concat,
                arguments: vec![make_literal_str("hello"), make_literal_str(" world")],
            })],
        });

        match ExpressionEvaluator::evaluate(&expr, &ctx).unwrap() {
            ColumnData::Int64(v) => assert_eq!(v[0], 11),
            _ => panic!("Expected Int64"),
        }
    }

    #[test]
    fn test_nested_arithmetic() {
        let ctx = EvaluationContext::new(HashMap::new(), 1);

        // (1 + 2) * 3 = 9
        let expr = ColumnExpression::BinaryOperation(ColumnarBinaryOperation {
            operator: BinaryOperator::Multiply,
            left_operand: Box::new(ColumnExpression::BinaryOperation(ColumnarBinaryOperation {
                operator: BinaryOperator::Add,
                left_operand: Box::new(make_literal_int(1)),
                right_operand: Box::new(make_literal_int(2)),
            })),
            right_operand: Box::new(make_literal_int(3)),
        });

        match ExpressionEvaluator::evaluate(&expr, &ctx).unwrap() {
            ColumnData::Int64(v) => assert_eq!(v[0], 9),
            _ => panic!("Expected Int64"),
        }
    }
}
