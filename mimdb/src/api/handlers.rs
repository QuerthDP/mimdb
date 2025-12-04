/*
 * Copyright (c) 2025-present Dawid Pawlik
 *
 * For educational use only by employees and students of MIMUW.
 * See LICENSE file for details.
 */

//! # HTTP Request Handlers
//!
//! This module contains all HTTP endpoint handlers for the MIMDB REST API.

use crate::api::OPENAPI_SPEC;
use crate::api::executor::QueryExecutor;
use crate::api::models::*;
use crate::metastore::ColumnMetadata;
use crate::metastore::Metastore;
use axum::Router;
use axum::extract::Path;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::response::Json;
use axum::routing::delete;
use axum::routing::get;
use axum::routing::post;
use axum::routing::put;
use std::sync::Arc;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::instrument;
use tracing::warn;

/// Application state shared across handlers
pub struct AppState {
    pub metastore: Arc<Metastore>,
    pub executor: Arc<QueryExecutor>,
    pub start_time: chrono::DateTime<chrono::Utc>,
}

// ============================================================================
// Table Endpoints
// ============================================================================

/// GET /tables - Get list of all tables
#[instrument(skip(state))]
async fn get_tables(State(state): State<Arc<AppState>>) -> Json<Vec<ShallowTable>> {
    debug!("Listing all tables");
    let tables: Vec<ShallowTable> = state
        .metastore
        .list_tables()
        .into_iter()
        .map(|(table_id, name)| ShallowTable { table_id, name })
        .collect();

    info!(count = tables.len(), "Retrieved tables list");
    Json(tables)
}

/// GET /table/{tableId} - Get detailed table information
#[instrument(skip(state))]
async fn get_table_by_id(
    State(state): State<Arc<AppState>>,
    Path(table_id): Path<String>,
) -> impl IntoResponse {
    debug!(table_id = %table_id, "Getting table details");
    match state.metastore.get_table(&table_id) {
        Some(table) => {
            info!(table_id = %table_id, table_name = %table.name, columns = table.columns.len(), "Table found");
            let schema = TableSchema {
                name: table.name,
                columns: table
                    .columns
                    .into_iter()
                    .map(|c| Column {
                        name: c.name,
                        column_type: c.column_type.into(),
                    })
                    .collect(),
            };
            (StatusCode::OK, Json(serde_json::to_value(schema).unwrap())).into_response()
        }
        None => {
            warn!(table_id = %table_id, "Table not found");
            (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse::new(format!(
                    "Table with ID '{}' not found",
                    table_id
                ))),
            )
                .into_response()
        }
    }
}

/// PUT /table - Create a new table
#[instrument(skip(state), fields(table_name = %schema.name))]
async fn create_table(
    State(state): State<Arc<AppState>>,
    Json(schema): Json<TableSchema>,
) -> impl IntoResponse {
    info!(table_name = %schema.name, columns = schema.columns.len(), "Creating new table");
    // Validate request
    let mut problems = Vec::new();

    if schema.name.is_empty() {
        problems.push(Problem {
            error: "Table name cannot be empty".to_string(),
            context: Some("name".to_string()),
        });
    }

    if schema.columns.is_empty() {
        problems.push(Problem {
            error: "Table must have at least one column".to_string(),
            context: Some("columns".to_string()),
        });
    }

    // Check for empty column names
    for (i, col) in schema.columns.iter().enumerate() {
        if col.name.is_empty() {
            problems.push(Problem {
                error: "Column name cannot be empty".to_string(),
                context: Some(format!("columns[{}]", i)),
            });
        }
    }

    if !problems.is_empty() {
        warn!(table_name = %schema.name, problems = ?problems, "Table creation validation failed");
        return (
            StatusCode::BAD_REQUEST,
            Json(MultipleProblemsError { problems }),
        )
            .into_response();
    }

    // Convert to internal representation
    let columns: Vec<ColumnMetadata> = schema
        .columns
        .into_iter()
        .map(|c| ColumnMetadata {
            name: c.name,
            column_type: c.column_type.into(),
        })
        .collect();

    match state.metastore.create_table(schema.name, columns) {
        Ok(table) => {
            info!(table_id = %table.table_id, "Table created successfully");
            (StatusCode::OK, Json(table.table_id)).into_response()
        }
        Err(e) => {
            error!(error = %e, "Failed to create table");
            (
                StatusCode::BAD_REQUEST,
                Json(MultipleProblemsError::single(e.to_string())),
            )
                .into_response()
        }
    }
}

/// DELETE /table/{tableId} - Delete a table
#[instrument(skip(state))]
async fn delete_table(
    State(state): State<Arc<AppState>>,
    Path(table_id): Path<String>,
) -> impl IntoResponse {
    info!(table_id = %table_id, "Deleting table");
    match state.metastore.delete_table(&table_id) {
        Ok(_) => {
            info!(table_id = %table_id, "Table deleted successfully");
            StatusCode::OK.into_response()
        }
        Err(e) => {
            warn!(table_id = %table_id, error = %e, "Failed to delete table");
            (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse::new(e.to_string())),
            )
                .into_response()
        }
    }
}

// ============================================================================
// Query Endpoints
// ============================================================================

/// GET /queries - Get list of all queries
#[instrument(skip(state))]
async fn get_queries(State(state): State<Arc<AppState>>) -> Json<Vec<ShallowQuery>> {
    debug!("Listing all queries");
    let queries: Vec<ShallowQuery> = state
        .executor
        .list_queries()
        .into_iter()
        .map(|(query_id, status)| ShallowQuery { query_id, status })
        .collect();

    info!(count = queries.len(), "Retrieved queries list");
    Json(queries)
}

/// GET /query/{queryId} - Get detailed query information
#[instrument(skip(state))]
async fn get_query_by_id(
    State(state): State<Arc<AppState>>,
    Path(query_id): Path<String>,
) -> impl IntoResponse {
    debug!(query_id = %query_id, "Getting query details");
    match state.executor.get_query(&query_id) {
        Some(query_state) => {
            let is_result_available = query_state.result.is_some();
            info!(query_id = %query_id, status = ?query_state.status, result_available = is_result_available, "Query found");
            let query = Query {
                query_id: query_state.query_id,
                status: query_state.status,
                is_result_available,
                query_definition: query_state.definition,
            };
            (StatusCode::OK, Json(query)).into_response()
        }
        None => {
            warn!(query_id = %query_id, "Query not found");
            (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse::new(format!(
                    "Query with ID '{}' not found",
                    query_id
                ))),
            )
                .into_response()
        }
    }
}

/// POST /query - Submit a new query for execution
#[instrument(skip(state, request), fields(query_type = ?request.query_definition))]
async fn submit_query(
    State(state): State<Arc<AppState>>,
    Json(request): Json<ExecuteQueryRequest>,
) -> impl IntoResponse {
    info!(query_definition = ?request.query_definition, "Submitting new query");
    match state.executor.submit_query(request.query_definition) {
        Ok(query_id) => {
            info!(query_id = %query_id, "Query submitted successfully");
            (StatusCode::OK, Json(query_id)).into_response()
        }
        Err(e) => {
            error!(error = %e, "Failed to submit query");
            (
                StatusCode::BAD_REQUEST,
                Json(MultipleProblemsError::single(e.to_string())),
            )
                .into_response()
        }
    }
}

// ============================================================================
// Result Endpoints
// ============================================================================

/// GET /result/{queryId} - Get result of a completed query
#[instrument(skip(state, body))]
async fn get_query_result(
    State(state): State<Arc<AppState>>,
    Path(query_id): Path<String>,
    body: Option<Json<GetQueryResultRequest>>,
) -> impl IntoResponse {
    let request = body.map(|b| b.0).unwrap_or_default();
    debug!(query_id = %query_id, row_limit = ?request.row_limit, "Getting query result");

    // First check if query exists
    let query = match state.executor.get_query(&query_id) {
        Some(q) => q,
        None => {
            warn!(query_id = %query_id, "Query not found when fetching result");
            return (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse::new(format!(
                    "Query with ID '{}' not found",
                    query_id
                ))),
            )
                .into_response();
        }
    };

    // Check if this is a SELECT query
    match &query.definition {
        QueryDefinition::Select(_) => {}
        QueryDefinition::Copy(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse::new("COPY queries do not have results")),
            )
                .into_response();
        }
    }

    // Get the result
    match state.executor.get_result(&query_id, request.row_limit) {
        Ok(Some(result)) => {
            // Flush result if requested
            if request.flush_result.unwrap_or(false) {
                if let Err(e) = state.executor.clear_result(&query_id) {
                    warn!(query_id = %query_id, error = %e, "Failed to flush query result");
                } else {
                    debug!(query_id = %query_id, "Query result flushed");
                }
            }
            info!(query_id = %query_id, rows = result.len(), "Query result retrieved");
            (StatusCode::OK, Json(result)).into_response()
        }
        Ok(None) => {
            warn!(query_id = %query_id, "Result not available for query");
            (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse::new("Result is not available for this query")),
            )
                .into_response()
        }
        Err(e) => {
            error!(query_id = %query_id, error = %e, "Failed to get query result");
            (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse::new(e.to_string())),
            )
                .into_response()
        }
    }
}

// ============================================================================
// Error Endpoints
// ============================================================================

/// GET /error/{queryId} - Get error of a failed query
#[instrument(skip(state))]
async fn get_query_error(
    State(state): State<Arc<AppState>>,
    Path(query_id): Path<String>,
) -> impl IntoResponse {
    debug!(query_id = %query_id, "Getting query error");
    // First check if query exists
    let query = match state.executor.get_query(&query_id) {
        Some(q) => q,
        None => {
            return (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse::new(format!(
                    "Query with ID '{}' not found",
                    query_id
                ))),
            )
                .into_response();
        }
    };

    // Check if query has failed
    if query.status != QueryStatus::Failed {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse::new(
                "Error is only available for failed queries",
            )),
        )
            .into_response();
    }

    match state.executor.get_error(&query_id) {
        Ok(Some(errors)) => {
            let problems: Vec<Problem> = errors
                .into_iter()
                .map(|e| Problem {
                    error: e,
                    context: None,
                })
                .collect();
            (StatusCode::OK, Json(MultipleProblemsError { problems })).into_response()
        }
        Ok(None) => (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse::new("No error information available")),
        )
            .into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse::new(e.to_string())),
        )
            .into_response(),
    }
}

// ============================================================================
// System Endpoints
// ============================================================================

/// Extract the interface version from the OpenAPI spec
fn get_interface_version() -> String {
    for line in OPENAPI_SPEC.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("version:") {
            return trimmed
                .trim_start_matches("version:")
                .trim()
                .trim_matches('"')
                .trim_matches('\'')
                .to_string();
        }
    }
    "unknown".to_string()
}

/// GET /system/info - Get system information
#[instrument(skip(state))]
async fn get_system_info(State(state): State<Arc<AppState>>) -> Json<SystemInformation> {
    debug!("Getting system information");
    let uptime_seconds = (chrono::Utc::now() - state.start_time).num_seconds();

    Json(SystemInformation {
        interface_version: get_interface_version(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        author: "Dawid Pawlik".to_string(),
        uptime: uptime_seconds,
    })
}

/// Create all routes
pub fn create_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/tables", get(get_tables))
        .route("/table/{tableId}", get(get_table_by_id))
        .route("/table", put(create_table))
        .route("/table/{tableId}", delete(delete_table))
        .route("/queries", get(get_queries))
        .route("/query/{queryId}", get(get_query_by_id))
        .route("/query", post(submit_query))
        .route("/result/{queryId}", get(get_query_result))
        .route("/error/{queryId}", get(get_query_error))
        .route("/system/info", get(get_system_info))
}
