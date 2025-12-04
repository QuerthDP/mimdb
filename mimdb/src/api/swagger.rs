/*
 * Copyright (c) 2025-present Dawid Pawlik
 *
 * For educational use only by employees and students of MIMUW.
 * See LICENSE file for details.
 */

//! # Swagger UI
//!
//! Serves the OpenAPI specification and Swagger UI for API documentation.

use crate::api::OPENAPI_SPEC;
use crate::api::handlers::AppState;
use axum::Router;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::response::Json;
use axum::routing::get;
use std::sync::Arc;
use utoipa_swagger_ui::SwaggerUi;

/// GET /api-docs/openapi.json - OpenAPI specification as JSON
async fn openapi_spec_json() -> impl IntoResponse {
    match serde_yaml::from_str::<serde_json::Value>(OPENAPI_SPEC) {
        Ok(spec) => (StatusCode::OK, Json(spec)).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to parse OpenAPI spec: {}", e),
        )
            .into_response(),
    }
}

/// Create Swagger UI routes
pub fn create_swagger_routes() -> Router<Arc<AppState>> {
    let swagger_config = utoipa_swagger_ui::Config::from("/api-docs/openapi.json");

    Router::new()
        .route("/api-docs/openapi.json", get(openapi_spec_json))
        .merge(SwaggerUi::new("/swagger-ui").config(swagger_config))
}
