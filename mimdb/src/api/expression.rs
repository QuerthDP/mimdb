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
//!
//! ## Common Subexpression Elimination (CSE)
//!
//! This module also provides CSE optimization which detects repeated expressions
//! and ensures they are computed only once. CSE works by:
//! 1. Computing a hash for each expression subtree
//! 2. For commutative operators, normalizing operand order by hash
//! 3. Identifying expressions that appear in both WHERE and SELECT clauses
//! 4. Computing shared expressions once in the input projection phase

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
use std::collections::HashSet;
use std::collections::hash_map::DefaultHasher;
use std::hash::Hash;
use std::hash::Hasher;

/// Context for expression evaluation - provides access to column data
pub struct EvaluationContext<'a> {
    /// Column data indexed by (table_name, column_name)
    pub columns: HashMap<(String, String), &'a ColumnData>,
    /// Total number of rows in the data
    pub row_count: usize,
    /// Default table name to use when column reference has no table name
    pub default_table: Option<String>,
}

impl<'a> EvaluationContext<'a> {
    pub fn new(columns: HashMap<(String, String), &'a ColumnData>, row_count: usize) -> Self {
        Self {
            columns,
            row_count,
            default_table: None,
        }
    }

    pub fn with_default_table(
        columns: HashMap<(String, String), &'a ColumnData>,
        row_count: usize,
        default_table: String,
    ) -> Self {
        Self {
            columns,
            row_count,
            default_table: Some(default_table),
        }
    }

    pub fn get_column_data(
        &self,
        table_name: Option<&str>,
        column_name: &str,
    ) -> Result<&'a ColumnData> {
        let resolved_table = table_name
            .map(|s| s.to_string())
            .or_else(|| self.default_table.clone())
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Column '{}' has no table name and no default table is available",
                    column_name
                )
            })?;

        let key = (resolved_table.clone(), column_name.to_string());
        self.columns
            .get(&key)
            .copied()
            .ok_or_else(|| anyhow::anyhow!("Column '{}.{}' not found", resolved_table, column_name))
    }
}

/// Expression evaluator - evaluates expressions column-wise (all rows at once)
/// with support for Common Subexpression Elimination (CSE).
pub struct ExpressionEvaluator;

impl ExpressionEvaluator {
    /// Evaluate an expression with CSE memoization.
    /// If the expression result is already in the CSE context, return the cached result.
    /// Otherwise, compute and cache it.
    pub fn evaluate(
        expr: &ColumnExpression,
        ctx: &EvaluationContext,
        cse: &mut CseContext,
    ) -> Result<ColumnData> {
        if let Some(cached) = cse.get(expr) {
            return Ok(cached.clone());
        }

        let result = match expr {
            ColumnExpression::Literal(lit) => Self::evaluate_literal(lit, ctx.row_count),
            ColumnExpression::ColumnReference(col_ref) => {
                Self::evaluate_column_reference(col_ref, ctx)
            }
            ColumnExpression::Function(func) => Self::evaluate_function(func, ctx, cse),
            ColumnExpression::BinaryOperation(bin_op) => Self::evaluate_binary_op(bin_op, ctx, cse),
            ColumnExpression::UnaryOperation(unary_op) => {
                Self::evaluate_unary_op(unary_op, ctx, cse)
            }
        }?;

        cse.insert(expr, result.clone());

        Ok(result)
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
        let data = ctx.get_column_data(col_ref.table_name.as_deref(), &col_ref.column_name)?;
        Ok(data.clone())
    }

    /// Evaluate a function call
    fn evaluate_function(
        func: &Function,
        ctx: &EvaluationContext,
        cse: &mut CseContext,
    ) -> Result<ColumnData> {
        match func.function_name {
            FunctionName::Strlen => {
                if func.arguments.len() != 1 {
                    anyhow::bail!("STRLEN expects exactly 1 argument");
                }
                let arg = Self::evaluate(&func.arguments[0], ctx, cse)?;
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
                let left = Self::evaluate(&func.arguments[0], ctx, cse)?;
                let right = Self::evaluate(&func.arguments[1], ctx, cse)?;
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
            FunctionName::Replace => {
                if func.arguments.len() != 3 {
                    anyhow::bail!("REPLACE expects exactly 3 arguments");
                }
                let source = Self::evaluate(&func.arguments[0], ctx, cse)?;
                let old = Self::evaluate(&func.arguments[1], ctx, cse)?;
                let new = Self::evaluate(&func.arguments[2], ctx, cse)?;
                match (source, old, new) {
                    (
                        ColumnData::Varchar(v_source),
                        ColumnData::Varchar(v_old),
                        ColumnData::Varchar(v_new),
                    ) => Ok(ColumnData::Varchar(
                        v_source
                            .iter()
                            .zip(v_old.iter())
                            .zip(v_new.iter())
                            .map(|((source_str, old_str), new_str)| {
                                source_str.replace(old_str, new_str)
                            })
                            .collect(),
                    )),
                    _ => anyhow::bail!("REPLACE expects VARCHAR arguments"),
                }
            }
            FunctionName::Upper => {
                if func.arguments.len() != 1 {
                    anyhow::bail!("UPPER expects exactly 1 argument");
                }
                let arg = Self::evaluate(&func.arguments[0], ctx, cse)?;
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
                let arg = Self::evaluate(&func.arguments[0], ctx, cse)?;
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
        cse: &mut CseContext,
    ) -> Result<ColumnData> {
        let left = Self::evaluate(&op.left_operand, ctx, cse)?;
        let right = Self::evaluate(&op.right_operand, ctx, cse)?;

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
        cse: &mut CseContext,
    ) -> Result<ColumnData> {
        let operand = Self::evaluate(&op.operand, ctx, cse)?;

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
        default_table: Option<&str>,
    ) -> Result<ColumnType> {
        match expr {
            ColumnExpression::Literal(lit) => match &lit.value {
                LiteralValue::Int64(_) => Ok(ColumnType::Int64),
                LiteralValue::Varchar(_) => Ok(ColumnType::Varchar),
                LiteralValue::Bool(_) => Ok(ColumnType::Bool),
            },
            ColumnExpression::ColumnReference(col_ref) => {
                let table_name =
                    col_ref
                        .table_name
                        .as_deref()
                        .or(default_table)
                        .ok_or_else(|| {
                            anyhow::anyhow!(
                                "Column '{}' has no table name and no default table is available",
                                col_ref.column_name
                            )
                        })?;
                let key = (table_name.to_string(), col_ref.column_name.clone());
                column_types.get(&key).cloned().ok_or_else(|| {
                    anyhow::anyhow!("Column '{}.{}' not found", table_name, col_ref.column_name)
                })
            }
            ColumnExpression::Function(func) => match func.function_name {
                FunctionName::Strlen => Ok(ColumnType::Int64),
                FunctionName::Concat
                | FunctionName::Replace
                | FunctionName::Upper
                | FunctionName::Lower => Ok(ColumnType::Varchar),
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
pub fn collect_table_references(expr: &ColumnExpression) -> HashSet<String> {
    let mut tables = HashSet::new();
    collect_table_references_recursive(expr, &mut tables);
    tables
}

fn collect_table_references_recursive(expr: &ColumnExpression, tables: &mut HashSet<String>) {
    match expr {
        ColumnExpression::ColumnReference(col_ref) => {
            if let Some(ref table_name) = col_ref.table_name {
                tables.insert(table_name.clone());
            }
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

/// Collect all column names referenced in an expression
pub fn collect_column_references(expr: &ColumnExpression) -> HashSet<String> {
    let mut columns = HashSet::new();
    collect_column_references_recursive(expr, &mut columns);
    columns
}

fn collect_column_references_recursive(expr: &ColumnExpression, columns: &mut HashSet<String>) {
    match expr {
        ColumnExpression::ColumnReference(col_ref) => {
            columns.insert(col_ref.column_name.clone());
        }
        ColumnExpression::Function(func) => {
            for arg in &func.arguments {
                collect_column_references_recursive(arg, columns);
            }
        }
        ColumnExpression::BinaryOperation(bin_op) => {
            collect_column_references_recursive(&bin_op.left_operand, columns);
            collect_column_references_recursive(&bin_op.right_operand, columns);
        }
        ColumnExpression::UnaryOperation(unary_op) => {
            collect_column_references_recursive(&unary_op.operand, columns);
        }
        ColumnExpression::Literal(_) => {}
    }
}

/// Compute a canonical hash for an expression.
/// For commutative operators (Add, Multiply, And, Or, Equal, NotEqual),
/// we sort operand hashes to ensure consistent hashing regardless of operand order.
fn compute_expression_hash(expr: &ColumnExpression) -> u64 {
    let mut hasher = DefaultHasher::new();
    compute_expression_hash_recursive(expr, &mut hasher);
    hasher.finish()
}

fn compute_expression_hash_recursive<H: Hasher>(expr: &ColumnExpression, hasher: &mut H) {
    match expr {
        ColumnExpression::Literal(lit) => {
            0u8.hash(hasher);
            lit.value.hash(hasher);
        }
        ColumnExpression::ColumnReference(col_ref) => {
            1u8.hash(hasher);
            col_ref.table_name.hash(hasher);
            col_ref.column_name.hash(hasher);
        }
        ColumnExpression::Function(func) => {
            2u8.hash(hasher);
            func.function_name.hash(hasher);
            for arg in &func.arguments {
                compute_expression_hash_recursive(arg, hasher);
            }
        }
        ColumnExpression::BinaryOperation(bin_op) => {
            3u8.hash(hasher);
            bin_op.operator.hash(hasher);

            let is_commutative = matches!(
                bin_op.operator,
                BinaryOperator::Add
                    | BinaryOperator::Multiply
                    | BinaryOperator::And
                    | BinaryOperator::Or
                    | BinaryOperator::Equal
                    | BinaryOperator::NotEqual
            );

            if is_commutative {
                let left_hash = compute_expression_hash(&bin_op.left_operand);
                let right_hash = compute_expression_hash(&bin_op.right_operand);
                let (first, second) = if left_hash <= right_hash {
                    (left_hash, right_hash)
                } else {
                    (right_hash, left_hash)
                };
                first.hash(hasher);
                second.hash(hasher);
            } else {
                compute_expression_hash_recursive(&bin_op.left_operand, hasher);
                compute_expression_hash_recursive(&bin_op.right_operand, hasher);
            }
        }
        ColumnExpression::UnaryOperation(unary_op) => {
            4u8.hash(hasher);
            unary_op.operator.hash(hasher);
            compute_expression_hash_recursive(&unary_op.operand, hasher);
        }
    }
}

/// Collect all subexpressions from an expression tree.
/// Returns a HashMap of expression hash -> expression reference.
/// This allows identifying common subexpressions by their hash.
pub fn collect_subexpressions(expr: &ColumnExpression) -> HashMap<u64, ColumnExpression> {
    let mut subexprs = HashMap::new();
    collect_subexpressions_recursive(expr, &mut subexprs);
    subexprs
}

fn collect_subexpressions_recursive(
    expr: &ColumnExpression,
    subexprs: &mut HashMap<u64, ColumnExpression>,
) {
    let hash = compute_expression_hash(expr);
    subexprs.entry(hash).or_insert_with(|| expr.clone());

    match expr {
        ColumnExpression::Literal(_) | ColumnExpression::ColumnReference(_) => {}
        ColumnExpression::Function(func) => {
            for arg in &func.arguments {
                collect_subexpressions_recursive(arg, subexprs);
            }
        }
        ColumnExpression::BinaryOperation(bin_op) => {
            collect_subexpressions_recursive(&bin_op.left_operand, subexprs);
            collect_subexpressions_recursive(&bin_op.right_operand, subexprs);
        }
        ColumnExpression::UnaryOperation(unary_op) => {
            collect_subexpressions_recursive(&unary_op.operand, subexprs);
        }
    }
}

/// CSE context for memoized expression evaluation.
/// Stores computed results indexed by expression hash.
pub struct CseContext {
    /// Computed expression results indexed by hash
    pub computed: HashMap<u64, ColumnData>,
}

impl CseContext {
    pub fn new() -> Self {
        Self {
            computed: HashMap::new(),
        }
    }

    /// Check if an expression result is already computed
    pub fn get(&self, expr: &ColumnExpression) -> Option<&ColumnData> {
        let hash = compute_expression_hash(expr);
        self.computed.get(&hash)
    }

    /// Store a computed expression result
    pub fn insert(&mut self, expr: &ColumnExpression, data: ColumnData) {
        let hash = compute_expression_hash(expr);
        self.computed.insert(hash, data);
    }
}

impl Default for CseContext {
    fn default() -> Self {
        Self::new()
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
        match ExpressionEvaluator::evaluate(&int_lit, &ctx, &mut CseContext::new()).unwrap() {
            ColumnData::Int64(v) => assert_eq!(v, vec![42, 42, 42]),
            _ => panic!("Expected Int64"),
        }

        let str_lit = make_literal_str("hello");
        match ExpressionEvaluator::evaluate(&str_lit, &ctx, &mut CseContext::new()).unwrap() {
            ColumnData::Varchar(v) => assert_eq!(v, vec!["hello", "hello", "hello"]),
            _ => panic!("Expected Varchar"),
        }

        let bool_lit = make_literal_bool(true);
        match ExpressionEvaluator::evaluate(&bool_lit, &ctx, &mut CseContext::new()).unwrap() {
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
        match ExpressionEvaluator::evaluate(&add, &ctx, &mut CseContext::new()).unwrap() {
            ColumnData::Int64(v) => assert_eq!(v[0], 15),
            _ => panic!("Expected Int64"),
        }

        // 10 - 3 = 7
        let sub = ColumnExpression::BinaryOperation(ColumnarBinaryOperation {
            operator: BinaryOperator::Subtract,
            left_operand: Box::new(make_literal_int(10)),
            right_operand: Box::new(make_literal_int(3)),
        });
        match ExpressionEvaluator::evaluate(&sub, &ctx, &mut CseContext::new()).unwrap() {
            ColumnData::Int64(v) => assert_eq!(v[0], 7),
            _ => panic!("Expected Int64"),
        }

        // 6 * 7 = 42
        let mul = ColumnExpression::BinaryOperation(ColumnarBinaryOperation {
            operator: BinaryOperator::Multiply,
            left_operand: Box::new(make_literal_int(6)),
            right_operand: Box::new(make_literal_int(7)),
        });
        match ExpressionEvaluator::evaluate(&mul, &ctx, &mut CseContext::new()).unwrap() {
            ColumnData::Int64(v) => assert_eq!(v[0], 42),
            _ => panic!("Expected Int64"),
        }

        // 20 / 4 = 5
        let div = ColumnExpression::BinaryOperation(ColumnarBinaryOperation {
            operator: BinaryOperator::Divide,
            left_operand: Box::new(make_literal_int(20)),
            right_operand: Box::new(make_literal_int(4)),
        });
        match ExpressionEvaluator::evaluate(&div, &ctx, &mut CseContext::new()).unwrap() {
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
        assert!(ExpressionEvaluator::evaluate(&div, &ctx, &mut CseContext::new()).is_err());
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
        match ExpressionEvaluator::evaluate(&and_op, &ctx, &mut CseContext::new()).unwrap() {
            ColumnData::Bool(v) => assert!(!v[0]),
            _ => panic!("Expected Bool"),
        }

        // true OR false = true
        let or_op = ColumnExpression::BinaryOperation(ColumnarBinaryOperation {
            operator: BinaryOperator::Or,
            left_operand: Box::new(make_literal_bool(true)),
            right_operand: Box::new(make_literal_bool(false)),
        });
        match ExpressionEvaluator::evaluate(&or_op, &ctx, &mut CseContext::new()).unwrap() {
            ColumnData::Bool(v) => assert!(v[0]),
            _ => panic!("Expected Bool"),
        }

        // NOT true = false
        let not_op = ColumnExpression::UnaryOperation(ColumnarUnaryOperation {
            operator: UnaryOperator::Not,
            operand: Box::new(make_literal_bool(true)),
        });
        match ExpressionEvaluator::evaluate(&not_op, &ctx, &mut CseContext::new()).unwrap() {
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
        match ExpressionEvaluator::evaluate(&eq, &ctx, &mut CseContext::new()).unwrap() {
            ColumnData::Bool(v) => assert!(v[0]),
            _ => panic!("Expected Bool"),
        }

        // 5 != 3
        let neq = ColumnExpression::BinaryOperation(ColumnarBinaryOperation {
            operator: BinaryOperator::NotEqual,
            left_operand: Box::new(make_literal_int(5)),
            right_operand: Box::new(make_literal_int(3)),
        });
        match ExpressionEvaluator::evaluate(&neq, &ctx, &mut CseContext::new()).unwrap() {
            ColumnData::Bool(v) => assert!(v[0]),
            _ => panic!("Expected Bool"),
        }

        // 3 < 5
        let lt = ColumnExpression::BinaryOperation(ColumnarBinaryOperation {
            operator: BinaryOperator::LessThan,
            left_operand: Box::new(make_literal_int(3)),
            right_operand: Box::new(make_literal_int(5)),
        });
        match ExpressionEvaluator::evaluate(&lt, &ctx, &mut CseContext::new()).unwrap() {
            ColumnData::Bool(v) => assert!(v[0]),
            _ => panic!("Expected Bool"),
        }

        // "abc" < "abd" (lexicographic)
        let str_lt = ColumnExpression::BinaryOperation(ColumnarBinaryOperation {
            operator: BinaryOperator::LessThan,
            left_operand: Box::new(make_literal_str("abc")),
            right_operand: Box::new(make_literal_str("abd")),
        });
        match ExpressionEvaluator::evaluate(&str_lt, &ctx, &mut CseContext::new()).unwrap() {
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
        match ExpressionEvaluator::evaluate(&strlen, &ctx, &mut CseContext::new()).unwrap() {
            ColumnData::Int64(v) => assert_eq!(v[0], 5),
            _ => panic!("Expected Int64"),
        }

        // CONCAT("hello", " world") = "hello world"
        let concat = ColumnExpression::Function(Function {
            function_name: FunctionName::Concat,
            arguments: vec![make_literal_str("hello"), make_literal_str(" world")],
        });
        match ExpressionEvaluator::evaluate(&concat, &ctx, &mut CseContext::new()).unwrap() {
            ColumnData::Varchar(v) => assert_eq!(v[0], "hello world"),
            _ => panic!("Expected Varchar"),
        }

        // UPPER("hello") = "HELLO"
        let upper = ColumnExpression::Function(Function {
            function_name: FunctionName::Upper,
            arguments: vec![make_literal_str("hello")],
        });
        match ExpressionEvaluator::evaluate(&upper, &ctx, &mut CseContext::new()).unwrap() {
            ColumnData::Varchar(v) => assert_eq!(v[0], "HELLO"),
            _ => panic!("Expected Varchar"),
        }

        // LOWER("HELLO") = "hello"
        let lower = ColumnExpression::Function(Function {
            function_name: FunctionName::Lower,
            arguments: vec![make_literal_str("HELLO")],
        });
        match ExpressionEvaluator::evaluate(&lower, &ctx, &mut CseContext::new()).unwrap() {
            ColumnData::Varchar(v) => assert_eq!(v[0], "hello"),
            _ => panic!("Expected Varchar"),
        }

        // REPLACE("hello world", "world", "Rust") = "hello Rust"
        let replace = ColumnExpression::Function(Function {
            function_name: FunctionName::Replace,
            arguments: vec![
                make_literal_str("hello world"),
                make_literal_str("world"),
                make_literal_str("Rust"),
            ],
        });
        match ExpressionEvaluator::evaluate(&replace, &ctx, &mut CseContext::new()).unwrap() {
            ColumnData::Varchar(v) => assert_eq!(v[0], "hello Rust"),
            _ => panic!("Expected Varchar"),
        }

        // REPLACE("aaa", "a", "aa") = "aaaaaa" (exponential growth)
        let replace_exp = ColumnExpression::Function(Function {
            function_name: FunctionName::Replace,
            arguments: vec![
                make_literal_str("aaa"),
                make_literal_str("a"),
                make_literal_str("aa"),
            ],
        });
        match ExpressionEvaluator::evaluate(&replace_exp, &ctx, &mut CseContext::new()).unwrap() {
            ColumnData::Varchar(v) => assert_eq!(v[0], "aaaaaa"),
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
        match ExpressionEvaluator::evaluate(&minus, &ctx, &mut CseContext::new()).unwrap() {
            ColumnData::Int64(v) => assert_eq!(v[0], -42),
            _ => panic!("Expected Int64"),
        }
    }

    #[test]
    fn test_column_reference() {
        let data = ColumnData::Int64(vec![10, 20, 30]);
        let mut columns = HashMap::new();
        columns.insert(("users".to_string(), "age".to_string()), &data);
        let ctx = EvaluationContext::with_default_table(columns, 3, "users".to_string());

        let col_ref = ColumnExpression::ColumnReference(ColumnReferenceExpression {
            table_name: Some("users".to_string()),
            column_name: "age".to_string(),
        });

        match ExpressionEvaluator::evaluate(&col_ref, &ctx, &mut CseContext::new()).unwrap() {
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

        match ExpressionEvaluator::evaluate(&expr, &ctx, &mut CseContext::new()).unwrap() {
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

        match ExpressionEvaluator::evaluate(&expr, &ctx, &mut CseContext::new()).unwrap() {
            ColumnData::Int64(v) => assert_eq!(v[0], 9),
            _ => panic!("Expected Int64"),
        }
    }

    /// Find common subexpressions between multiple expression trees.
    /// Returns a set of expression hashes that appear in multiple trees.
    fn find_common_subexpressions(expressions: &[&ColumnExpression]) -> HashSet<u64> {
        let mut hash_counts: HashMap<u64, usize> = HashMap::new();

        for expr in expressions {
            let subexprs = collect_subexpressions(expr);
            for hash in subexprs.keys() {
                *hash_counts.entry(*hash).or_default() += 1;
            }
        }

        hash_counts
            .into_iter()
            .filter(|&(_, count)| count > 1)
            .map(|(hash, _)| hash)
            .collect()
    }

    #[test]
    fn test_cse_expression_hash_consistency() {
        // Same expression should produce same hash
        let expr1 = make_literal_int(42);
        let expr2 = make_literal_int(42);

        let hash1 = super::compute_expression_hash(&expr1);
        let hash2 = super::compute_expression_hash(&expr2);

        assert_eq!(hash1, hash2, "Same expressions should have same hash");
    }

    #[test]
    fn test_cse_expression_hash_different() {
        // Different expressions should produce different hashes
        let expr1 = make_literal_int(42);
        let expr2 = make_literal_int(43);

        let hash1 = super::compute_expression_hash(&expr1);
        let hash2 = super::compute_expression_hash(&expr2);

        assert_ne!(
            hash1, hash2,
            "Different expressions should have different hash"
        );
    }

    #[test]
    fn test_cse_commutative_operator_hash() {
        // For commutative operators, operand order shouldn't matter
        let add1 = ColumnExpression::BinaryOperation(ColumnarBinaryOperation {
            operator: BinaryOperator::Add,
            left_operand: Box::new(make_literal_int(1)),
            right_operand: Box::new(make_literal_int(2)),
        });
        let add2 = ColumnExpression::BinaryOperation(ColumnarBinaryOperation {
            operator: BinaryOperator::Add,
            left_operand: Box::new(make_literal_int(2)),
            right_operand: Box::new(make_literal_int(1)),
        });

        let hash1 = super::compute_expression_hash(&add1);
        let hash2 = super::compute_expression_hash(&add2);

        assert_eq!(
            hash1, hash2,
            "Commutative operations should have same hash regardless of operand order"
        );
    }

    #[test]
    fn test_cse_non_commutative_operator_hash() {
        // For non-commutative operators, operand order SHOULD matter
        let sub1 = ColumnExpression::BinaryOperation(ColumnarBinaryOperation {
            operator: BinaryOperator::Subtract,
            left_operand: Box::new(make_literal_int(10)),
            right_operand: Box::new(make_literal_int(5)),
        });
        let sub2 = ColumnExpression::BinaryOperation(ColumnarBinaryOperation {
            operator: BinaryOperator::Subtract,
            left_operand: Box::new(make_literal_int(5)),
            right_operand: Box::new(make_literal_int(10)),
        });

        let hash1 = super::compute_expression_hash(&sub1);
        let hash2 = super::compute_expression_hash(&sub2);

        assert_ne!(
            hash1, hash2,
            "Non-commutative operations should have different hash when operands are swapped"
        );
    }

    #[test]
    fn test_cse_collect_subexpressions() {
        // Create nested expression: (1 + 2) * 3
        let inner = ColumnExpression::BinaryOperation(ColumnarBinaryOperation {
            operator: BinaryOperator::Add,
            left_operand: Box::new(make_literal_int(1)),
            right_operand: Box::new(make_literal_int(2)),
        });
        let outer = ColumnExpression::BinaryOperation(ColumnarBinaryOperation {
            operator: BinaryOperator::Multiply,
            left_operand: Box::new(inner.clone()),
            right_operand: Box::new(make_literal_int(3)),
        });

        let subexprs = super::collect_subexpressions(&outer);

        // Should find: outer, inner, literal 1, literal 2, literal 3
        assert_eq!(subexprs.len(), 5, "Should collect 5 subexpressions");

        // Verify inner expression is in the collection
        let inner_hash = super::compute_expression_hash(&inner);
        assert!(
            subexprs.contains_key(&inner_hash),
            "Should contain inner expression"
        );
    }

    #[test]
    fn test_cse_find_common_subexpressions() {
        // Create two expressions that share a common subexpression
        let common = ColumnExpression::BinaryOperation(ColumnarBinaryOperation {
            operator: BinaryOperator::Add,
            left_operand: Box::new(make_literal_int(1)),
            right_operand: Box::new(make_literal_int(2)),
        });

        // expr1 = common * 3
        let expr1 = ColumnExpression::BinaryOperation(ColumnarBinaryOperation {
            operator: BinaryOperator::Multiply,
            left_operand: Box::new(common.clone()),
            right_operand: Box::new(make_literal_int(3)),
        });

        // expr2 = common + 10
        let expr2 = ColumnExpression::BinaryOperation(ColumnarBinaryOperation {
            operator: BinaryOperator::Add,
            left_operand: Box::new(common.clone()),
            right_operand: Box::new(make_literal_int(10)),
        });

        let common_hashes = find_common_subexpressions(&[&expr1, &expr2]);

        // Should find common subexpression (1 + 2)
        let common_hash = super::compute_expression_hash(&common);
        assert!(
            common_hashes.contains(&common_hash),
            "Should detect common subexpression (1+2)"
        );
    }

    #[test]
    fn test_cse_evaluate_with_memoization() {
        let ctx = EvaluationContext::new(HashMap::new(), 1);
        let mut cse = super::CseContext::new();

        // Create expression that would be expensive if computed multiple times
        let expensive = ColumnExpression::Function(Function {
            function_name: FunctionName::Strlen,
            arguments: vec![make_literal_str("hello world")],
        });

        // First evaluation - should compute and cache
        let result1 = ExpressionEvaluator::evaluate(&expensive, &ctx, &mut cse).unwrap();

        // Verify result is cached
        let hash = super::compute_expression_hash(&expensive);
        assert!(
            cse.computed.contains_key(&hash),
            "Result should be cached after evaluation"
        );

        // Second evaluation - should use cached value
        let result2 = ExpressionEvaluator::evaluate(&expensive, &ctx, &mut cse).unwrap();

        match (result1, result2) {
            (ColumnData::Int64(v1), ColumnData::Int64(v2)) => {
                assert_eq!(v1, v2, "Both evaluations should produce same result");
                assert_eq!(v1[0], 11, "STRLEN('hello world') = 11");
            }
            _ => panic!("Expected Int64"),
        }
    }

    #[test]
    fn test_cse_with_repeated_subexpressions() {
        let ctx = EvaluationContext::new(HashMap::new(), 1);
        let mut cse = super::CseContext::new();

        // Create expression: STRLEN("test") + STRLEN("test")
        // Without CSE: STRLEN computed twice
        // With CSE: STRLEN computed once, reused
        let strlen_expr = ColumnExpression::Function(Function {
            function_name: FunctionName::Strlen,
            arguments: vec![make_literal_str("test")],
        });

        let add_expr = ColumnExpression::BinaryOperation(ColumnarBinaryOperation {
            operator: BinaryOperator::Add,
            left_operand: Box::new(strlen_expr.clone()),
            right_operand: Box::new(strlen_expr.clone()),
        });

        let result = ExpressionEvaluator::evaluate(&add_expr, &ctx, &mut cse).unwrap();

        match result {
            ColumnData::Int64(v) => {
                assert_eq!(v[0], 8, "STRLEN('test') + STRLEN('test') = 4 + 4 = 8");
            }
            _ => panic!("Expected Int64"),
        }

        // Verify STRLEN was cached (computed once)
        let strlen_hash = super::compute_expression_hash(&strlen_expr);
        assert!(
            cse.computed.contains_key(&strlen_hash),
            "STRLEN should be cached"
        );
    }

    #[test]
    fn test_cse_plan_detects_shared_expressions() {
        // This test verifies that common subexpressions between WHERE and SELECT
        // are properly detected

        // Common expression: STRLEN(CONCAT("a", "b"))
        let common = ColumnExpression::Function(Function {
            function_name: FunctionName::Strlen,
            arguments: vec![ColumnExpression::Function(Function {
                function_name: FunctionName::Concat,
                arguments: vec![make_literal_str("a"), make_literal_str("b")],
            })],
        });

        // WHERE: common > 0
        let where_expr = ColumnExpression::BinaryOperation(ColumnarBinaryOperation {
            operator: BinaryOperator::GreaterThan,
            left_operand: Box::new(common.clone()),
            right_operand: Box::new(make_literal_int(0)),
        });

        // SELECT: common + 10
        let select_expr = ColumnExpression::BinaryOperation(ColumnarBinaryOperation {
            operator: BinaryOperator::Add,
            left_operand: Box::new(common.clone()),
            right_operand: Box::new(make_literal_int(10)),
        });

        // Collect all expressions
        let expressions: Vec<&ColumnExpression> = vec![&where_expr, &select_expr];
        let common_hashes = find_common_subexpressions(&expressions);

        // Verify the shared STRLEN(CONCAT("a", "b")) is detected
        let common_hash = super::compute_expression_hash(&common);
        assert!(
            common_hashes.contains(&common_hash),
            "Should detect STRLEN(CONCAT(...)) as common subexpression between WHERE and SELECT"
        );

        // Print visualization of detected common expressions (as suggested in instructions)
        println!("CSE Plan Visualization:");
        println!("  Detected {} common subexpressions", common_hashes.len());
        println!("  Common expression hash: {}", common_hash);
    }
}
