/*
 * Copyright (c) 2025-present Dawid Pawlik
 *
 * For educational use only by employees and students of MIMUW.
 * See LICENSE file for details.
 */

//! # API Data Models
//!
//! Data structures for the REST API, corresponding to the OpenAPI schema
//! defined in dbmsInterface.yaml.

use serde::Deserialize;
use serde::Serialize;

// ============================================================================
// Column Types
// ============================================================================

/// Logical column type enum
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum LogicalColumnType {
    Int64,
    Varchar,
}

impl From<crate::ColumnType> for LogicalColumnType {
    fn from(ct: crate::ColumnType) -> Self {
        match ct {
            crate::ColumnType::Int64 => LogicalColumnType::Int64,
            crate::ColumnType::Varchar => LogicalColumnType::Varchar,
        }
    }
}

impl From<LogicalColumnType> for crate::ColumnType {
    fn from(lct: LogicalColumnType) -> Self {
        match lct {
            LogicalColumnType::Int64 => crate::ColumnType::Int64,
            LogicalColumnType::Varchar => crate::ColumnType::Varchar,
        }
    }
}

// ============================================================================
// Table Schema
// ============================================================================

/// Description of a single column in a table
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    #[serde(rename = "type")]
    pub column_type: LogicalColumnType,
}

/// Description of the table in the database
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<Column>,
}

/// Shallow representation of a table (without detailed column information)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ShallowTable {
    pub table_id: String,
    pub name: String,
}

// ============================================================================
// Query Status and Types
// ============================================================================

/// Possible query statuses
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum QueryStatus {
    Created,
    Planning,
    Running,
    Completed,
    Failed,
}

/// Shallow representation of a query
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ShallowQuery {
    pub query_id: String,
    pub status: QueryStatus,
}

/// COPY query definition
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CopyQuery {
    pub source_filepath: String,
    pub destination_table_name: String,
    #[serde(default)]
    pub destination_columns: Option<Vec<String>>,
    #[serde(default)]
    pub does_csv_contain_header: bool,
}

/// SELECT query definition
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SelectQuery {
    pub column_clauses: Vec<ColumnExpression>,
    #[serde(default)]
    pub where_clause: Option<ColumnExpression>,
    #[serde(default)]
    pub order_by_clause: Option<Vec<OrderByExpression>>,
    #[serde(default)]
    pub limit_clause: Option<LimitExpression>,
}

// ============================================================================
// Column Expressions
// ============================================================================

/// Column expression - describes a single column in a SELECT query
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ColumnExpression {
    BinaryOperation(ColumnarBinaryOperation),
    UnaryOperation(ColumnarUnaryOperation),
    Function(Function),
    ColumnReference(ColumnReferenceExpression),
    Literal(Literal),
}

/// Reference to a column in a table
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ColumnReferenceExpression {
    pub table_name: String,
    pub column_name: String,
}

/// Literal value
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Literal {
    pub value: LiteralValue,
}

/// The actual value of a literal
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum LiteralValue {
    Int64(i64),
    Bool(bool),
    Varchar(String),
}

/// Function types supported in column expressions
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum FunctionName {
    Strlen,
    Concat,
    Upper,
    Lower,
}

/// Function call in a column expression
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Function {
    pub function_name: FunctionName,
    pub arguments: Vec<ColumnExpression>,
}

/// Binary operators for column expressions
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum BinaryOperator {
    Add,
    Subtract,
    Multiply,
    Divide,
    And,
    Or,
    Equal,
    NotEqual,
    LessThan,
    LessEqual,
    GreaterThan,
    GreaterEqual,
}

/// Binary operation in a column expression
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ColumnarBinaryOperation {
    pub operator: BinaryOperator,
    pub left_operand: Box<ColumnExpression>,
    pub right_operand: Box<ColumnExpression>,
}

/// Unary operators for column expressions
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum UnaryOperator {
    Not,
    Minus,
}

/// Unary operation in a column expression
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ColumnarUnaryOperation {
    pub operator: UnaryOperator,
    pub operand: Box<ColumnExpression>,
}

/// ORDER BY expression
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderByExpression {
    pub column_index: usize,
    #[serde(default = "default_ascending")]
    pub ascending: bool,
}

fn default_ascending() -> bool {
    true
}

/// LIMIT expression
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LimitExpression {
    pub limit: i32,
}

/// Query definition - either COPY or SELECT
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum QueryDefinition {
    Copy(CopyQuery),
    Select(SelectQuery),
}

/// Full query description
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Query {
    pub query_id: String,
    pub status: QueryStatus,
    pub is_result_available: bool,
    pub query_definition: QueryDefinition,
}

// ============================================================================
// Request/Response Bodies
// ============================================================================

/// Request to execute a query
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecuteQueryRequest {
    pub query_definition: QueryDefinition,
}

/// Request to get query result
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct GetQueryResultRequest {
    #[serde(default)]
    pub row_limit: Option<i32>,
    #[serde(default)]
    pub flush_result: Option<bool>,
}

// ============================================================================
// Query Result
// ============================================================================

/// Column data in query result
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ResultColumn {
    Int64(Vec<i64>),
    Varchar(Vec<String>),
}

/// Single query result item (QueryResult is an array of these)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryResultItem {
    pub row_count: i32,
    pub columns: Vec<ResultColumn>,
}

/// Query result structure - array of result items as per OpenAPI spec
pub type QueryResult = Vec<QueryResultItem>;

// ============================================================================
// Error Types
// ============================================================================

/// Single problem in error response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Problem {
    pub error: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context: Option<String>,
}

/// Error response with multiple problems
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultipleProblemsError {
    pub problems: Vec<Problem>,
}

impl MultipleProblemsError {
    pub fn single(error: impl Into<String>) -> Self {
        Self {
            problems: vec![Problem {
                error: error.into(),
                context: None,
            }],
        }
    }

    pub fn with_context(error: impl Into<String>, context: impl Into<String>) -> Self {
        Self {
            problems: vec![Problem {
                error: error.into(),
                context: Some(context.into()),
            }],
        }
    }
}

/// Generic error response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorResponse {
    pub message: String,
}

impl ErrorResponse {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

// ============================================================================
// System Information
// ============================================================================

/// System information response
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SystemInformation {
    pub interface_version: String,
    pub version: String,
    pub author: String,
    pub uptime: i64,
}
