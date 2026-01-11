/*
 * Copyright (c) 2025-present Dawid Pawlik
 *
 * For educational use only by employees and students of MIMUW.
 * See LICENSE file for details.
 */

//! # Public Interface Testing (PIT) for MIMDB REST API
//!
//! This module provides comprehensive end-to-end tests for the MIMDB REST API.
//! Tests validate the system through its public HTTP interface, simulating
//! real-world usage patterns.
//!
//! ## Test Categories:
//! - Table operations (CREATE, LIST, GET, DELETE)
//! - Query operations (COPY, SELECT)
//! - Result retrieval
//! - Error handling
//! - Persistence across restarts

use axum::Router;
use axum_test::TestServer;
use mimdb::api::executor::QueryExecutor;
use mimdb::api::handlers::AppState;
use mimdb::api::handlers::create_routes;
use mimdb::metastore::Metastore;
use std::sync::Arc;
use tempfile::TempDir;

/// Helper function to create a test server
fn create_test_server(temp_dir: &TempDir) -> TestServer {
    let metastore = Arc::new(Metastore::new(temp_dir.path()).unwrap());
    let executor = Arc::new(QueryExecutor::new(Arc::clone(&metastore)));

    let app_state = Arc::new(AppState {
        metastore,
        executor,
        start_time: chrono::Utc::now(),
    });

    let app: Router = create_routes().with_state(app_state);
    TestServer::new(app).unwrap()
}

/// Helper function to wait for a query to complete by polling the API
async fn wait_for_query_completion(server: &TestServer, query_id: &str) {
    for _ in 0..100 {
        let resp = server.get(&format!("/query/{}", query_id)).await;
        let query: serde_json::Value = resp.json();
        let status = query["status"].as_str().unwrap_or("");
        if status == "COMPLETED" || status == "FAILED" {
            return;
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    }
    panic!("Query did not complete in time");
}

// ============================================================================
// System Info Tests
// ============================================================================

#[tokio::test]
async fn test_system_info() {
    let temp_dir = TempDir::new().unwrap();
    let server = create_test_server(&temp_dir);

    let resp = server.get("/system/info").await;
    resp.assert_status_success();

    let body: serde_json::Value = resp.json();
    assert!(body.get("interfaceVersion").is_some());
    assert!(body.get("version").is_some());
    assert!(body.get("author").is_some());
}

// ============================================================================
// Table Operations Tests
// ============================================================================

#[tokio::test]
async fn test_list_tables_empty() {
    let temp_dir = TempDir::new().unwrap();
    let server = create_test_server(&temp_dir);

    let resp = server.get("/tables").await;
    resp.assert_status_success();

    let body: Vec<serde_json::Value> = resp.json();
    assert!(body.is_empty());
}

#[tokio::test]
async fn test_create_table() {
    let temp_dir = TempDir::new().unwrap();
    let server = create_test_server(&temp_dir);

    let table_schema = serde_json::json!({
        "name": "users",
        "columns": [
            {"name": "id", "type": "INT64"},
            {"name": "name", "type": "VARCHAR"}
        ]
    });

    let resp = server.put("/table").json(&table_schema).await;
    resp.assert_status_success();

    let table_id: String = resp.json();
    assert!(!table_id.is_empty());

    // Verify table was created
    let resp = server.get("/tables").await;
    let tables: Vec<serde_json::Value> = resp.json();

    assert_eq!(tables.len(), 1);
    assert_eq!(tables[0]["name"], "users");
}

#[tokio::test]
async fn test_create_table_duplicate_name() {
    let temp_dir = TempDir::new().unwrap();
    let server = create_test_server(&temp_dir);

    let table_schema = serde_json::json!({
        "name": "users",
        "columns": [
            {"name": "id", "type": "INT64"}
        ]
    });

    // First creation should succeed
    let resp = server.put("/table").json(&table_schema).await;
    resp.assert_status_success();

    // Second creation with same name should fail
    let resp = server.put("/table").json(&table_schema).await;
    resp.assert_status_bad_request();
}

#[tokio::test]
async fn test_create_table_duplicate_columns() {
    let temp_dir = TempDir::new().unwrap();
    let server = create_test_server(&temp_dir);

    let table_schema = serde_json::json!({
        "name": "users",
        "columns": [
            {"name": "id", "type": "INT64"},
            {"name": "id", "type": "VARCHAR"}  // Duplicate column name
        ]
    });

    let resp = server.put("/table").json(&table_schema).await;
    resp.assert_status_bad_request();
}

#[tokio::test]
async fn test_get_table_by_id() {
    let temp_dir = TempDir::new().unwrap();
    let server = create_test_server(&temp_dir);

    // Create table
    let table_schema = serde_json::json!({
        "name": "products",
        "columns": [
            {"name": "id", "type": "INT64"},
            {"name": "name", "type": "VARCHAR"},
            {"name": "price", "type": "INT64"}
        ]
    });

    let resp = server.put("/table").json(&table_schema).await;
    let table_id: String = resp.json();

    // Get table details
    let resp = server.get(&format!("/table/{}", table_id)).await;
    resp.assert_status_success();

    let body: serde_json::Value = resp.json();
    assert_eq!(body["name"], "products");
    assert_eq!(body["columns"].as_array().unwrap().len(), 3);
}

#[tokio::test]
async fn test_get_nonexistent_table() {
    let temp_dir = TempDir::new().unwrap();
    let server = create_test_server(&temp_dir);

    let resp = server.get("/table/nonexistent-id").await;
    resp.assert_status_not_found();
}

#[tokio::test]
async fn test_delete_table() {
    let temp_dir = TempDir::new().unwrap();
    let server = create_test_server(&temp_dir);

    // Create table
    let table_schema = serde_json::json!({
        "name": "temp_table",
        "columns": [
            {"name": "id", "type": "INT64"}
        ]
    });

    let resp = server.put("/table").json(&table_schema).await;
    let table_id: String = resp.json();

    // Delete table
    let resp = server.delete(&format!("/table/{}", table_id)).await;
    resp.assert_status_success();

    // Verify table is deleted
    let resp = server.get("/tables").await;
    let tables: Vec<serde_json::Value> = resp.json();
    assert!(tables.is_empty());
}

// ============================================================================
// Query Operations Tests
// ============================================================================

#[tokio::test]
async fn test_list_queries_empty() {
    let temp_dir = TempDir::new().unwrap();
    let server = create_test_server(&temp_dir);

    let resp = server.get("/queries").await;
    resp.assert_status_success();

    let body: Vec<serde_json::Value> = resp.json();
    assert!(body.is_empty());
}

#[tokio::test]
async fn test_select_query_on_empty_table() {
    let temp_dir = TempDir::new().unwrap();
    let server = create_test_server(&temp_dir);

    // Create table
    let table_schema = serde_json::json!({
        "name": "empty_table",
        "columns": [
            {"name": "id", "type": "INT64"},
            {"name": "value", "type": "VARCHAR"}
        ]
    });

    server.put("/table").json(&table_schema).await;

    // Execute SELECT query
    let query = serde_json::json!({
        "queryDefinition": {
            "columnClauses": [
                {"tableName": "empty_table", "columnName": "id"},
                {"tableName": "empty_table", "columnName": "value"}
            ]
        }
    });

    let resp = server.post("/query").json(&query).await;
    resp.assert_status_success();

    let query_id: String = resp.json();

    // Wait for query to complete
    wait_for_query_completion(&server, &query_id).await;

    // Get result
    let resp = server.get(&format!("/result/{}", query_id)).await;
    resp.assert_status_success();

    let result: serde_json::Value = resp.json();
    assert!(result.is_array());
    assert_eq!(result[0]["rowCount"], 0);
}

#[tokio::test]
async fn test_select_nonexistent_table() {
    let temp_dir = TempDir::new().unwrap();
    let server = create_test_server(&temp_dir);

    let query = serde_json::json!({
        "queryDefinition": {
            "columnClauses": [
                {"tableName": "nonexistent_table", "columnName": "id"}
            ]
        }
    });

    let resp = server.post("/query").json(&query).await;
    resp.assert_status_bad_request();
}

#[tokio::test]
async fn test_copy_and_select_full_workflow() {
    let temp_dir = TempDir::new().unwrap();
    let server = create_test_server(&temp_dir);

    // 1. Create table
    let table_schema = serde_json::json!({
        "name": "employees",
        "columns": [
            {"name": "id", "type": "INT64"},
            {"name": "name", "type": "VARCHAR"},
            {"name": "salary", "type": "INT64"}
        ]
    });

    let resp = server.put("/table").json(&table_schema).await;
    resp.assert_status_success();

    // 2. Create CSV file
    let csv_path = temp_dir.path().join("employees.csv");
    std::fs::write(&csv_path, "1,Alice,50000\n2,Bob,60000\n3,Charlie,55000\n").unwrap();

    // 3. Execute COPY query
    let copy_query = serde_json::json!({
        "queryDefinition": {
            "sourceFilepath": csv_path.to_str().unwrap(),
            "destinationTableName": "employees",
            "doesCsvContainHeader": false
        }
    });

    let resp = server.post("/query").json(&copy_query).await;
    resp.assert_status_success();

    let copy_query_id: String = resp.json();

    // 4. Wait for and check COPY query status
    wait_for_query_completion(&server, &copy_query_id).await;
    let resp = server.get(&format!("/query/{}", copy_query_id)).await;
    let query_status: serde_json::Value = resp.json();
    assert_eq!(query_status["status"], "COMPLETED");

    // 5. Execute SELECT query
    let select_query = serde_json::json!({
        "queryDefinition": {
            "columnClauses": [
                {"tableName": "employees", "columnName": "id"},
                {"tableName": "employees", "columnName": "name"},
                {"tableName": "employees", "columnName": "salary"}
            ]
        }
    });

    let resp = server.post("/query").json(&select_query).await;
    resp.assert_status_success();

    let select_query_id: String = resp.json();

    // 6. Wait for SELECT and get result
    wait_for_query_completion(&server, &select_query_id).await;
    let resp = server.get(&format!("/result/{}", select_query_id)).await;
    resp.assert_status_success();

    let result: serde_json::Value = resp.json();
    assert!(result.is_array());
    assert_eq!(result[0]["rowCount"], 3);
    assert_eq!(result[0]["columns"].as_array().unwrap().len(), 3);
}

#[tokio::test]
async fn test_copy_with_header() {
    let temp_dir = TempDir::new().unwrap();
    let server = create_test_server(&temp_dir);

    // Create table
    let table_schema = serde_json::json!({
        "name": "products",
        "columns": [
            {"name": "id", "type": "INT64"},
            {"name": "name", "type": "VARCHAR"}
        ]
    });

    server.put("/table").json(&table_schema).await;

    // Create CSV with header
    let csv_path = temp_dir.path().join("products.csv");
    std::fs::write(&csv_path, "id,name\n1,Apple\n2,Banana\n").unwrap();

    // Execute COPY with header flag
    let copy_query = serde_json::json!({
        "queryDefinition": {
            "sourceFilepath": csv_path.to_str().unwrap(),
            "destinationTableName": "products",
            "doesCsvContainHeader": true
        }
    });

    let resp = server.post("/query").json(&copy_query).await;
    resp.assert_status_success();

    let copy_query_id: String = resp.json();
    wait_for_query_completion(&server, &copy_query_id).await;

    // Execute SELECT
    let select_query = serde_json::json!({
        "queryDefinition": {
            "columnClauses": [
                {"tableName": "products", "columnName": "id"},
                {"tableName": "products", "columnName": "name"}
            ]
        }
    });

    let resp = server.post("/query").json(&select_query).await;
    let select_query_id: String = resp.json();

    // Wait for SELECT and get result - should have 2 rows (header not counted as data)
    wait_for_query_completion(&server, &select_query_id).await;
    let resp = server.get(&format!("/result/{}", select_query_id)).await;
    let result: serde_json::Value = resp.json();

    assert!(result.is_array());
    assert_eq!(result[0]["rowCount"], 2);
}

#[tokio::test]
async fn test_copy_nonexistent_file() {
    let temp_dir = TempDir::new().unwrap();
    let server = create_test_server(&temp_dir);

    // Create table
    let table_schema = serde_json::json!({
        "name": "test_table",
        "columns": [
            {"name": "id", "type": "INT64"}
        ]
    });

    server.put("/table").json(&table_schema).await;

    // Try COPY from nonexistent file
    let copy_query = serde_json::json!({
        "queryDefinition": {
            "sourceFilepath": "/nonexistent/path/file.csv",
            "destinationTableName": "test_table",
            "doesCsvContainHeader": false
        }
    });

    let resp = server.post("/query").json(&copy_query).await;
    resp.assert_status_bad_request();
}

// ============================================================================
// Result Row Limit Tests
// ============================================================================

#[tokio::test]
async fn test_result_with_row_limit() {
    let temp_dir = TempDir::new().unwrap();
    let server = create_test_server(&temp_dir);

    // Create and populate table
    let table_schema = serde_json::json!({
        "name": "numbers",
        "columns": [
            {"name": "value", "type": "INT64"}
        ]
    });

    server.put("/table").json(&table_schema).await;

    // Create CSV with 10 rows
    let csv_path = temp_dir.path().join("numbers.csv");
    let csv_content: String = (1..=10).map(|i| format!("{}\n", i)).collect();
    std::fs::write(&csv_path, csv_content).unwrap();

    // COPY data
    let copy_query = serde_json::json!({
        "queryDefinition": {
            "sourceFilepath": csv_path.to_str().unwrap(),
            "destinationTableName": "numbers",
            "doesCsvContainHeader": false
        }
    });

    let resp = server.post("/query").json(&copy_query).await;
    let copy_query_id: String = resp.json();
    wait_for_query_completion(&server, &copy_query_id).await;

    // Execute SELECT
    let select_query = serde_json::json!({
        "queryDefinition": {
            "columnClauses": [
                {"tableName": "numbers", "columnName": "value"}
            ]
        }
    });

    let resp = server.post("/query").json(&select_query).await;
    let select_query_id: String = resp.json();

    // Wait for SELECT to complete
    wait_for_query_completion(&server, &select_query_id).await;

    // Get result with limit
    let resp = server
        .get(&format!("/result/{}", select_query_id))
        .json(&serde_json::json!({"rowLimit": 5}))
        .await;
    let result: serde_json::Value = resp.json();

    assert!(result.is_array());
    assert_eq!(result[0]["rowCount"], 5);
    assert_eq!(result[0]["columns"][0].as_array().unwrap().len(), 5);
}

// ============================================================================
// Persistence Tests
// ============================================================================

#[tokio::test]
async fn test_persistence_across_restarts() {
    let temp_dir = TempDir::new().unwrap();

    // First "session" - create table and add data
    {
        let server = create_test_server(&temp_dir);

        // Create table
        let table_schema = serde_json::json!({
            "name": "persistent_table",
            "columns": [
                {"name": "id", "type": "INT64"},
                {"name": "data", "type": "VARCHAR"}
            ]
        });

        server.put("/table").json(&table_schema).await;

        // Add data via COPY
        let csv_path = temp_dir.path().join("data.csv");
        std::fs::write(&csv_path, "1,test_data\n2,more_data\n").unwrap();

        let copy_query = serde_json::json!({
            "queryDefinition": {
                "sourceFilepath": csv_path.to_str().unwrap(),
                "destinationTableName": "persistent_table",
                "doesCsvContainHeader": false
            }
        });

        let resp = server.post("/query").json(&copy_query).await;
        let copy_query_id: String = resp.json();
        wait_for_query_completion(&server, &copy_query_id).await;
    }

    // Second "session" - verify data persisted
    {
        let server = create_test_server(&temp_dir);

        // Table should exist
        let resp = server.get("/tables").await;
        let tables: Vec<serde_json::Value> = resp.json();

        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0]["name"], "persistent_table");

        // Data should be queryable
        let select_query = serde_json::json!({
            "queryDefinition": {
                "columnClauses": [
                    {"tableName": "persistent_table", "columnName": "id"},
                    {"tableName": "persistent_table", "columnName": "data"}
                ]
            }
        });

        let resp = server.post("/query").json(&select_query).await;
        let select_query_id: String = resp.json();

        wait_for_query_completion(&server, &select_query_id).await;
        let resp = server.get(&format!("/result/{}", select_query_id)).await;
        let result: serde_json::Value = resp.json();

        assert!(result.is_array());
        assert_eq!(result[0]["rowCount"], 2);
    }
}

// ============================================================================
// Error Handling Tests
// ============================================================================

#[tokio::test]
async fn test_invalid_json_request() {
    let temp_dir = TempDir::new().unwrap();
    let server = create_test_server(&temp_dir);

    let resp = server
        .put("/table")
        .content_type("application/json")
        .bytes("invalid json".into())
        .await;

    assert!(resp.status_code().is_client_error());
}

#[tokio::test]
async fn test_empty_table_name() {
    let temp_dir = TempDir::new().unwrap();
    let server = create_test_server(&temp_dir);

    let table_schema = serde_json::json!({
        "name": "",
        "columns": [
            {"name": "id", "type": "INT64"}
        ]
    });

    let resp = server.put("/table").json(&table_schema).await;
    resp.assert_status_bad_request();
}

#[tokio::test]
async fn test_table_with_no_columns() {
    let temp_dir = TempDir::new().unwrap();
    let server = create_test_server(&temp_dir);

    let table_schema = serde_json::json!({
        "name": "empty_columns",
        "columns": []
    });

    let resp = server.put("/table").json(&table_schema).await;
    resp.assert_status_bad_request();
}

// ============================================================================
// Multiple COPY Operations Test
// ============================================================================

#[tokio::test]
async fn test_multiple_copy_operations() {
    let temp_dir = TempDir::new().unwrap();
    let server = create_test_server(&temp_dir);

    // Create table
    let table_schema = serde_json::json!({
        "name": "logs",
        "columns": [
            {"name": "id", "type": "INT64"},
            {"name": "message", "type": "VARCHAR"}
        ]
    });

    server.put("/table").json(&table_schema).await;

    // First COPY
    let csv_path1 = temp_dir.path().join("logs1.csv");
    std::fs::write(&csv_path1, "1,First\n2,Second\n").unwrap();

    let copy_query1 = serde_json::json!({
        "queryDefinition": {
            "sourceFilepath": csv_path1.to_str().unwrap(),
            "destinationTableName": "logs",
            "doesCsvContainHeader": false
        }
    });

    let resp = server.post("/query").json(&copy_query1).await;
    let copy_query_id1: String = resp.json();
    wait_for_query_completion(&server, &copy_query_id1).await;

    // Second COPY
    let csv_path2 = temp_dir.path().join("logs2.csv");
    std::fs::write(&csv_path2, "3,Third\n4,Fourth\n5,Fifth\n").unwrap();

    let copy_query2 = serde_json::json!({
        "queryDefinition": {
            "sourceFilepath": csv_path2.to_str().unwrap(),
            "destinationTableName": "logs",
            "doesCsvContainHeader": false
        }
    });

    let resp = server.post("/query").json(&copy_query2).await;
    let copy_query_id2: String = resp.json();
    wait_for_query_completion(&server, &copy_query_id2).await;

    // SELECT should return all 5 rows
    let select_query = serde_json::json!({
        "queryDefinition": {
            "columnClauses": [
                {"tableName": "logs", "columnName": "id"},
                {"tableName": "logs", "columnName": "message"}
            ]
        }
    });

    let resp = server.post("/query").json(&select_query).await;
    let select_query_id: String = resp.json();

    wait_for_query_completion(&server, &select_query_id).await;
    let resp = server.get(&format!("/result/{}", select_query_id)).await;
    let result: serde_json::Value = resp.json();

    assert!(result.is_array());
    assert_eq!(result[0]["rowCount"], 5);
}

// ============================================================================
// Query Status Tests
// ============================================================================

#[tokio::test]
async fn test_query_status_completed() {
    let temp_dir = TempDir::new().unwrap();
    let server = create_test_server(&temp_dir);

    // Create table
    let table_schema = serde_json::json!({
        "name": "test",
        "columns": [{"name": "id", "type": "INT64"}]
    });

    server.put("/table").json(&table_schema).await;

    // Execute SELECT
    let select_query = serde_json::json!({
        "queryDefinition": {
            "columnClauses": [
                {"tableName": "test", "columnName": "id"}
            ]
        }
    });

    let resp = server.post("/query").json(&select_query).await;
    let query_id: String = resp.json();

    // Wait for completion then check status
    wait_for_query_completion(&server, &query_id).await;
    let resp = server.get(&format!("/query/{}", query_id)).await;
    let query: serde_json::Value = resp.json();

    assert_eq!(query["status"], "COMPLETED");
    assert_eq!(query["isResultAvailable"], true);
}

#[tokio::test]
async fn test_queries_list_after_operations() {
    let temp_dir = TempDir::new().unwrap();
    let server = create_test_server(&temp_dir);

    // Create table
    let table_schema = serde_json::json!({
        "name": "test",
        "columns": [{"name": "id", "type": "INT64"}]
    });

    server.put("/table").json(&table_schema).await;

    // Execute multiple queries
    let mut query_ids = Vec::new();
    for _ in 0..3 {
        let select_query = serde_json::json!({
            "queryDefinition": {
                "columnClauses": [
                    {"tableName": "test", "columnName": "id"}
                ]
            }
        });

        let resp = server.post("/query").json(&select_query).await;
        let query_id: String = resp.json();
        query_ids.push(query_id);
    }

    // Wait for all queries to complete
    for query_id in &query_ids {
        wait_for_query_completion(&server, query_id).await;
    }

    // Check queries list
    let resp = server.get("/queries").await;
    let queries: Vec<serde_json::Value> = resp.json();

    assert_eq!(queries.len(), 3);

    for query in queries {
        assert!(query.get("queryId").is_some());
        assert_eq!(query["status"], "COMPLETED");
    }
}

#[tokio::test]
async fn test_flush_result() {
    let temp_dir = TempDir::new().unwrap();
    let server = create_test_server(&temp_dir);

    // 1. Create table
    let table_schema = serde_json::json!({
        "name": "test_flush",
        "columns": [
            {"name": "id", "type": "INT64"},
            {"name": "value", "type": "VARCHAR"}
        ]
    });

    let resp = server.put("/table").json(&table_schema).await;
    resp.assert_status_success();

    // 2. Create CSV file and load data
    let csv_path = temp_dir.path().join("test_flush.csv");
    std::fs::write(&csv_path, "1,hello\n2,world\n").unwrap();

    let copy_query = serde_json::json!({
        "queryDefinition": {
            "sourceFilepath": csv_path.to_str().unwrap(),
            "destinationTableName": "test_flush",
            "doesCsvContainHeader": false
        }
    });

    let resp = server.post("/query").json(&copy_query).await;
    resp.assert_status_success();

    let copy_query_id: String = resp.json();
    wait_for_query_completion(&server, &copy_query_id).await;

    // 3. Execute SELECT query
    let select_query = serde_json::json!({
        "queryDefinition": {
            "columnClauses": [
                {"tableName": "test_flush", "columnName": "id"},
                {"tableName": "test_flush", "columnName": "value"}
            ]
        }
    });

    let resp = server.post("/query").json(&select_query).await;
    resp.assert_status_success();

    let query_id: String = resp.json();

    // Wait for SELECT to complete
    wait_for_query_completion(&server, &query_id).await;

    // 4. Get result WITHOUT flushing - should succeed
    let resp = server.get(&format!("/result/{}", query_id)).await;
    resp.assert_status_success();

    let result: serde_json::Value = resp.json();
    assert!(result.is_array());
    assert_eq!(result[0]["rowCount"], 2);

    // 5. Get result again - should still work (not flushed)
    let resp = server.get(&format!("/result/{}", query_id)).await;
    resp.assert_status_success();

    // 6. Get result WITH flushResult=true
    let flush_request = serde_json::json!({"flushResult": true});
    let resp = server
        .get(&format!("/result/{}", query_id))
        .json(&flush_request)
        .await;
    resp.assert_status_success();

    let result: serde_json::Value = resp.json();
    assert_eq!(result[0]["rowCount"], 2);

    // 7. Try to get result again - should fail (was flushed)
    let resp = server.get(&format!("/result/{}", query_id)).await;
    resp.assert_status_bad_request();

    let error: serde_json::Value = resp.json();
    assert!(error["message"].as_str().unwrap().contains("not available"));

    // 8. Query should still exist and show as completed
    let resp = server.get(&format!("/query/{}", query_id)).await;
    resp.assert_status_success();

    let query: serde_json::Value = resp.json();
    assert_eq!(query["status"], "COMPLETED");
    // isResultAvailable should now be false since we flushed
    assert_eq!(query["isResultAvailable"], false);
}

#[tokio::test]
async fn test_select_with_multiple_tables() {
    let temp_dir = TempDir::new().unwrap();
    let server = create_test_server(&temp_dir);

    // 1. Create first table
    let table1_schema = serde_json::json!({
        "name": "users",
        "columns": [
            {"name": "id", "type": "INT64"},
            {"name": "name", "type": "VARCHAR"}
        ]
    });

    let resp = server.put("/table").json(&table1_schema).await;
    resp.assert_status_success();

    // 2. Create second table
    let table2_schema = serde_json::json!({
        "name": "orders",
        "columns": [
            {"name": "id", "type": "INT64"},
            {"name": "user_id", "type": "INT64"}
        ]
    });

    let resp = server.put("/table").json(&table2_schema).await;
    resp.assert_status_success();

    // 3. Load data into first table
    let csv_path1 = temp_dir.path().join("users.csv");
    std::fs::write(&csv_path1, "1,Alice\n2,Bob\n").unwrap();

    let copy_query1 = serde_json::json!({
        "queryDefinition": {
            "sourceFilepath": csv_path1.to_str().unwrap(),
            "destinationTableName": "users",
            "doesCsvContainHeader": false
        }
    });

    let resp = server.post("/query").json(&copy_query1).await;
    resp.assert_status_success();

    let query_id1: String = resp.json();
    wait_for_query_completion(&server, &query_id1).await;

    // 4. Load data into second table
    let csv_path2 = temp_dir.path().join("orders.csv");
    std::fs::write(&csv_path2, "1,1\n2,1\n3,2\n").unwrap();

    let copy_query2 = serde_json::json!({
        "queryDefinition": {
            "sourceFilepath": csv_path2.to_str().unwrap(),
            "destinationTableName": "orders",
            "doesCsvContainHeader": false
        }
    });

    let resp = server.post("/query").json(&copy_query2).await;
    resp.assert_status_success();

    let query_id2: String = resp.json();
    wait_for_query_completion(&server, &query_id2).await;

    // 5. Try to SELECT from both tables (this should fail if joins are not supported)
    let select_query = serde_json::json!({
        "queryDefinition": {
            "columnClauses": [
                {"tableName": "users", "columnName": "id"},
                {"tableName": "users", "columnName": "name"},
                {"tableName": "orders", "columnName": "id"},
                {"tableName": "orders", "columnName": "user_id"}
            ]
        }
    });

    let resp = server.post("/query").json(&select_query).await;

    // Check if the query was submitted successfully
    if resp.status_code() == 200 {
        let query_id: String = resp.json();
        wait_for_query_completion(&server, &query_id).await;

        // Check if the query failed
        let resp = server.get(&format!("/query/{}", query_id)).await;
        let query: serde_json::Value = resp.json();

        // Expect the query to fail since multi-table queries are not supported
        assert_eq!(
            query["status"], "FAILED",
            "Query should fail when referencing multiple tables"
        );

        // Try to get the error details
        let resp = server.get(&format!("/error/{}", query_id)).await;
        resp.assert_status_success();

        let error: serde_json::Value = resp.json();
        println!("Error details: {:?}", error);
    } else {
        // If the query submission itself failed, that's also a valid way to reject multi-table queries
        resp.assert_status_bad_request();
        let error: serde_json::Value = resp.json();
        println!("Query submission failed: {:?}", error);
    }
}

// ============================================================================
// SELECT with WHERE, ORDER BY, LIMIT Tests
// ============================================================================

#[tokio::test]
async fn test_select_with_where_equals() {
    let temp_dir = TempDir::new().unwrap();
    let server = create_test_server(&temp_dir);

    // Create table
    let table_schema = serde_json::json!({
        "name": "orders",
        "columns": [
            {"name": "id", "type": "INT64"},
            {"name": "status", "type": "VARCHAR"}
        ]
    });

    server.put("/table").json(&table_schema).await;

    // Create CSV with mixed status values
    let csv_path = temp_dir.path().join("orders.csv");
    std::fs::write(
        &csv_path,
        "1,completed\n2,pending\n3,completed\n4,cancelled\n5,completed\n",
    )
    .unwrap();

    // COPY data
    let copy_query = serde_json::json!({
        "queryDefinition": {
            "sourceFilepath": csv_path.to_str().unwrap(),
            "destinationTableName": "orders",
            "doesCsvContainHeader": false
        }
    });

    let resp = server.post("/query").json(&copy_query).await;
    let copy_query_id: String = resp.json();
    wait_for_query_completion(&server, &copy_query_id).await;

    // SELECT with WHERE clause filtering for "completed" status
    let select_query = serde_json::json!({
        "queryDefinition": {
            "columnClauses": [
                {"tableName": "orders", "columnName": "id"},
                {"tableName": "orders", "columnName": "status"}
            ],
            "whereClause": {
                "operator": "EQUAL",
                "leftOperand": {"tableName": "orders", "columnName": "status"},
                "rightOperand": {"value": "completed"}
            }
        }
    });

    let resp = server.post("/query").json(&select_query).await;
    resp.assert_status_success();

    let select_query_id: String = resp.json();
    wait_for_query_completion(&server, &select_query_id).await;

    let resp = server.get(&format!("/result/{}", select_query_id)).await;
    resp.assert_status_success();

    let result: serde_json::Value = resp.json();
    assert!(result.is_array());
    // Should return 3 rows (id 1, 3, 5 with status "completed")
    assert_eq!(result[0]["rowCount"], 3);
}

#[tokio::test]
async fn test_select_with_where_greater_than() {
    let temp_dir = TempDir::new().unwrap();
    let server = create_test_server(&temp_dir);

    // Create table
    let table_schema = serde_json::json!({
        "name": "employees",
        "columns": [
            {"name": "id", "type": "INT64"},
            {"name": "salary", "type": "INT64"}
        ]
    });

    server.put("/table").json(&table_schema).await;

    // Create CSV with salary data
    let csv_path = temp_dir.path().join("employees.csv");
    std::fs::write(&csv_path, "1,50000\n2,75000\n3,60000\n4,100000\n5,55000\n").unwrap();

    // COPY data
    let copy_query = serde_json::json!({
        "queryDefinition": {
            "sourceFilepath": csv_path.to_str().unwrap(),
            "destinationTableName": "employees",
            "doesCsvContainHeader": false
        }
    });

    let resp = server.post("/query").json(&copy_query).await;
    let copy_query_id: String = resp.json();
    wait_for_query_completion(&server, &copy_query_id).await;

    // SELECT with WHERE salary > 60000
    let select_query = serde_json::json!({
        "queryDefinition": {
            "columnClauses": [
                {"tableName": "employees", "columnName": "id"},
                {"tableName": "employees", "columnName": "salary"}
            ],
            "whereClause": {
                "operator": "GREATER_THAN",
                "leftOperand": {"tableName": "employees", "columnName": "salary"},
                "rightOperand": {"value": 60000}
            }
        }
    });

    let resp = server.post("/query").json(&select_query).await;
    resp.assert_status_success();

    let select_query_id: String = resp.json();
    wait_for_query_completion(&server, &select_query_id).await;

    let resp = server.get(&format!("/result/{}", select_query_id)).await;
    resp.assert_status_success();

    let result: serde_json::Value = resp.json();
    assert!(result.is_array());
    // Should return 2 rows (id 2 with salary 75000, id 4 with salary 100000)
    assert_eq!(result[0]["rowCount"], 2);
}

#[tokio::test]
async fn test_select_with_where_no_matches() {
    let temp_dir = TempDir::new().unwrap();
    let server = create_test_server(&temp_dir);

    // Create table
    let table_schema = serde_json::json!({
        "name": "items",
        "columns": [
            {"name": "id", "type": "INT64"},
            {"name": "category", "type": "VARCHAR"}
        ]
    });

    server.put("/table").json(&table_schema).await;

    // Create CSV
    let csv_path = temp_dir.path().join("items.csv");
    std::fs::write(&csv_path, "1,book\n2,pen\n3,book\n").unwrap();

    // COPY data
    let copy_query = serde_json::json!({
        "queryDefinition": {
            "sourceFilepath": csv_path.to_str().unwrap(),
            "destinationTableName": "items",
            "doesCsvContainHeader": false
        }
    });

    let resp = server.post("/query").json(&copy_query).await;
    let copy_query_id: String = resp.json();
    wait_for_query_completion(&server, &copy_query_id).await;

    // SELECT with WHERE category = "laptop" (no matches)
    let select_query = serde_json::json!({
        "queryDefinition": {
            "columnClauses": [
                {"tableName": "items", "columnName": "id"},
                {"tableName": "items", "columnName": "category"}
            ],
            "whereClause": {
                "operator": "EQUAL",
                "leftOperand": {"tableName": "items", "columnName": "category"},
                "rightOperand": {"value": "laptop"}
            }
        }
    });

    let resp = server.post("/query").json(&select_query).await;
    resp.assert_status_success();

    let select_query_id: String = resp.json();
    wait_for_query_completion(&server, &select_query_id).await;

    let resp = server.get(&format!("/result/{}", select_query_id)).await;
    resp.assert_status_success();

    let result: serde_json::Value = resp.json();
    assert!(result.is_array());
    // Should return 0 rows
    assert_eq!(result[0]["rowCount"], 0);
}

#[tokio::test]
async fn test_select_with_order_by_ascending() {
    let temp_dir = TempDir::new().unwrap();
    let server = create_test_server(&temp_dir);

    // Create table
    let table_schema = serde_json::json!({
        "name": "products",
        "columns": [
            {"name": "id", "type": "INT64"},
            {"name": "price", "type": "INT64"}
        ]
    });

    server.put("/table").json(&table_schema).await;

    // Create CSV with unsorted prices
    let csv_path = temp_dir.path().join("products.csv");
    std::fs::write(&csv_path, "1,30\n2,10\n3,50\n4,20\n5,40\n").unwrap();

    // COPY data
    let copy_query = serde_json::json!({
        "queryDefinition": {
            "sourceFilepath": csv_path.to_str().unwrap(),
            "destinationTableName": "products",
            "doesCsvContainHeader": false
        }
    });

    let resp = server.post("/query").json(&copy_query).await;
    let copy_query_id: String = resp.json();
    wait_for_query_completion(&server, &copy_query_id).await;

    // SELECT with ORDER BY price ASC
    let select_query = serde_json::json!({
        "queryDefinition": {
            "columnClauses": [
                {"tableName": "products", "columnName": "id"},
                {"tableName": "products", "columnName": "price"}
            ],
            "orderByClause": [
                {"columnIndex": 1, "ascending": true}
            ]
        }
    });

    let resp = server.post("/query").json(&select_query).await;
    resp.assert_status_success();

    let select_query_id: String = resp.json();
    wait_for_query_completion(&server, &select_query_id).await;

    let resp = server.get(&format!("/result/{}", select_query_id)).await;
    resp.assert_status_success();

    let result: serde_json::Value = resp.json();
    assert!(result.is_array());
    assert_eq!(result[0]["rowCount"], 5);

    // Verify order: prices should be 10, 20, 30, 40, 50
    let prices = result[0]["columns"][1].as_array().unwrap();
    assert_eq!(prices[0], 10);
    assert_eq!(prices[1], 20);
    assert_eq!(prices[2], 30);
    assert_eq!(prices[3], 40);
    assert_eq!(prices[4], 50);
}

#[tokio::test]
async fn test_select_with_order_by_descending() {
    let temp_dir = TempDir::new().unwrap();
    let server = create_test_server(&temp_dir);

    // Create table
    let table_schema = serde_json::json!({
        "name": "scores",
        "columns": [
            {"name": "id", "type": "INT64"},
            {"name": "score", "type": "INT64"}
        ]
    });

    server.put("/table").json(&table_schema).await;

    // Create CSV
    let csv_path = temp_dir.path().join("scores.csv");
    std::fs::write(&csv_path, "1,85\n2,92\n3,78\n4,95\n5,88\n").unwrap();

    // COPY data
    let copy_query = serde_json::json!({
        "queryDefinition": {
            "sourceFilepath": csv_path.to_str().unwrap(),
            "destinationTableName": "scores",
            "doesCsvContainHeader": false
        }
    });

    let resp = server.post("/query").json(&copy_query).await;
    let copy_query_id: String = resp.json();
    wait_for_query_completion(&server, &copy_query_id).await;

    // SELECT with ORDER BY score DESC
    let select_query = serde_json::json!({
        "queryDefinition": {
            "columnClauses": [
                {"tableName": "scores", "columnName": "id"},
                {"tableName": "scores", "columnName": "score"}
            ],
            "orderByClause": [
                {"columnIndex": 1, "ascending": false}
            ]
        }
    });

    let resp = server.post("/query").json(&select_query).await;
    resp.assert_status_success();

    let select_query_id: String = resp.json();
    wait_for_query_completion(&server, &select_query_id).await;

    let resp = server.get(&format!("/result/{}", select_query_id)).await;
    resp.assert_status_success();

    let result: serde_json::Value = resp.json();
    assert!(result.is_array());
    assert_eq!(result[0]["rowCount"], 5);

    // Verify order: scores should be 95, 92, 88, 85, 78
    let scores = result[0]["columns"][1].as_array().unwrap();
    assert_eq!(scores[0], 95);
    assert_eq!(scores[1], 92);
    assert_eq!(scores[2], 88);
    assert_eq!(scores[3], 85);
    assert_eq!(scores[4], 78);
}

#[tokio::test]
async fn test_select_with_order_by_multiple_columns() {
    let temp_dir = TempDir::new().unwrap();
    let server = create_test_server(&temp_dir);

    // Create table
    let table_schema = serde_json::json!({
        "name": "students",
        "columns": [
            {"name": "id", "type": "INT64"},
            {"name": "grade", "type": "INT64"},
            {"name": "score", "type": "INT64"}
        ]
    });

    server.put("/table").json(&table_schema).await;

    // Create CSV with students in same grades with different scores
    let csv_path = temp_dir.path().join("students.csv");
    std::fs::write(&csv_path, "1,10,85\n2,11,90\n3,10,95\n4,11,80\n5,10,90\n").unwrap();

    // COPY data
    let copy_query = serde_json::json!({
        "queryDefinition": {
            "sourceFilepath": csv_path.to_str().unwrap(),
            "destinationTableName": "students",
            "doesCsvContainHeader": false
        }
    });

    let resp = server.post("/query").json(&copy_query).await;
    let copy_query_id: String = resp.json();
    wait_for_query_completion(&server, &copy_query_id).await;

    // SELECT with ORDER BY grade ASC, score DESC
    let select_query = serde_json::json!({
        "queryDefinition": {
            "columnClauses": [
                {"tableName": "students", "columnName": "id"},
                {"tableName": "students", "columnName": "grade"},
                {"tableName": "students", "columnName": "score"}
            ],
            "orderByClause": [
                {"columnIndex": 1, "ascending": true},
                {"columnIndex": 2, "ascending": false}
            ]
        }
    });

    let resp = server.post("/query").json(&select_query).await;
    resp.assert_status_success();

    let select_query_id: String = resp.json();
    wait_for_query_completion(&server, &select_query_id).await;

    let resp = server.get(&format!("/result/{}", select_query_id)).await;
    resp.assert_status_success();

    let result: serde_json::Value = resp.json();
    assert!(result.is_array());
    assert_eq!(result[0]["rowCount"], 5);

    // Verify order: grade 10 first (scores 95, 90, 85), then grade 11 (scores 90, 80)
    let grades = result[0]["columns"][1].as_array().unwrap();
    let scores = result[0]["columns"][2].as_array().unwrap();

    // Grade 10 students sorted by score DESC
    assert_eq!(grades[0], 10);
    assert_eq!(scores[0], 95);
    assert_eq!(grades[1], 10);
    assert_eq!(scores[1], 90);
    assert_eq!(grades[2], 10);
    assert_eq!(scores[2], 85);

    // Grade 11 students sorted by score DESC
    assert_eq!(grades[3], 11);
    assert_eq!(scores[3], 90);
    assert_eq!(grades[4], 11);
    assert_eq!(scores[4], 80);
}

#[tokio::test]
async fn test_select_with_limit_basic() {
    let temp_dir = TempDir::new().unwrap();
    let server = create_test_server(&temp_dir);

    // Create table
    let table_schema = serde_json::json!({
        "name": "numbers",
        "columns": [
            {"name": "value", "type": "INT64"}
        ]
    });

    server.put("/table").json(&table_schema).await;

    // Create CSV with 10 rows
    let csv_path = temp_dir.path().join("numbers.csv");
    std::fs::write(&csv_path, "1\n2\n3\n4\n5\n6\n7\n8\n9\n10\n").unwrap();

    // COPY data
    let copy_query = serde_json::json!({
        "queryDefinition": {
            "sourceFilepath": csv_path.to_str().unwrap(),
            "destinationTableName": "numbers",
            "doesCsvContainHeader": false
        }
    });

    let resp = server.post("/query").json(&copy_query).await;
    let copy_query_id: String = resp.json();
    wait_for_query_completion(&server, &copy_query_id).await;

    // SELECT with LIMIT 3
    let select_query = serde_json::json!({
        "queryDefinition": {
            "columnClauses": [
                {"tableName": "numbers", "columnName": "value"}
            ],
            "limitClause": {"limit": 3}
        }
    });

    let resp = server.post("/query").json(&select_query).await;
    resp.assert_status_success();

    let select_query_id: String = resp.json();
    wait_for_query_completion(&server, &select_query_id).await;

    let resp = server.get(&format!("/result/{}", select_query_id)).await;
    resp.assert_status_success();

    let result: serde_json::Value = resp.json();
    assert!(result.is_array());
    assert_eq!(result[0]["rowCount"], 3);
}

#[tokio::test]
async fn test_select_with_limit_zero() {
    let temp_dir = TempDir::new().unwrap();
    let server = create_test_server(&temp_dir);

    // Create table
    let table_schema = serde_json::json!({
        "name": "data",
        "columns": [
            {"name": "id", "type": "INT64"}
        ]
    });

    server.put("/table").json(&table_schema).await;

    // Create CSV
    let csv_path = temp_dir.path().join("data.csv");
    std::fs::write(&csv_path, "1\n2\n3\n").unwrap();

    // COPY data
    let copy_query = serde_json::json!({
        "queryDefinition": {
            "sourceFilepath": csv_path.to_str().unwrap(),
            "destinationTableName": "data",
            "doesCsvContainHeader": false
        }
    });

    let resp = server.post("/query").json(&copy_query).await;
    let copy_query_id: String = resp.json();
    wait_for_query_completion(&server, &copy_query_id).await;

    // SELECT with LIMIT 0
    let select_query = serde_json::json!({
        "queryDefinition": {
            "columnClauses": [
                {"tableName": "data", "columnName": "id"}
            ],
            "limitClause": {"limit": 0}
        }
    });

    let resp = server.post("/query").json(&select_query).await;
    resp.assert_status_success();

    let select_query_id: String = resp.json();
    wait_for_query_completion(&server, &select_query_id).await;

    let resp = server.get(&format!("/result/{}", select_query_id)).await;
    resp.assert_status_success();

    let result: serde_json::Value = resp.json();
    assert!(result.is_array());
    assert_eq!(result[0]["rowCount"], 0);
}

#[tokio::test]
async fn test_select_with_limit_exceeds_rows() {
    let temp_dir = TempDir::new().unwrap();
    let server = create_test_server(&temp_dir);

    // Create table
    let table_schema = serde_json::json!({
        "name": "small_table",
        "columns": [
            {"name": "id", "type": "INT64"}
        ]
    });

    server.put("/table").json(&table_schema).await;

    // Create CSV with only 3 rows
    let csv_path = temp_dir.path().join("small.csv");
    std::fs::write(&csv_path, "1\n2\n3\n").unwrap();

    // COPY data
    let copy_query = serde_json::json!({
        "queryDefinition": {
            "sourceFilepath": csv_path.to_str().unwrap(),
            "destinationTableName": "small_table",
            "doesCsvContainHeader": false
        }
    });

    let resp = server.post("/query").json(&copy_query).await;
    let copy_query_id: String = resp.json();
    wait_for_query_completion(&server, &copy_query_id).await;

    // SELECT with LIMIT 100 (more than available rows)
    let select_query = serde_json::json!({
        "queryDefinition": {
            "columnClauses": [
                {"tableName": "small_table", "columnName": "id"}
            ],
            "limitClause": {"limit": 100}
        }
    });

    let resp = server.post("/query").json(&select_query).await;
    resp.assert_status_success();

    let select_query_id: String = resp.json();
    wait_for_query_completion(&server, &select_query_id).await;

    let resp = server.get(&format!("/result/{}", select_query_id)).await;
    resp.assert_status_success();

    let result: serde_json::Value = resp.json();
    assert!(result.is_array());
    // Should return all 3 rows
    assert_eq!(result[0]["rowCount"], 3);
}

#[tokio::test]
async fn test_select_with_where_order_by_limit() {
    let temp_dir = TempDir::new().unwrap();
    let server = create_test_server(&temp_dir);

    // Create table
    let table_schema = serde_json::json!({
        "name": "items",
        "columns": [
            {"name": "id", "type": "INT64"},
            {"name": "category", "type": "VARCHAR"},
            {"name": "price", "type": "INT64"}
        ]
    });

    server.put("/table").json(&table_schema).await;

    // Create CSV
    let csv_path = temp_dir.path().join("items.csv");
    std::fs::write(
        &csv_path,
        "1,book,20\n2,pen,5\n3,book,15\n4,pen,8\n5,book,25\n6,pen,10\n7,book,12\n",
    )
    .unwrap();

    // COPY data
    let copy_query = serde_json::json!({
        "queryDefinition": {
            "sourceFilepath": csv_path.to_str().unwrap(),
            "destinationTableName": "items",
            "doesCsvContainHeader": false
        }
    });

    let resp = server.post("/query").json(&copy_query).await;
    let copy_query_id: String = resp.json();
    wait_for_query_completion(&server, &copy_query_id).await;

    // SELECT books with price DESC, LIMIT 2
    let select_query = serde_json::json!({
        "queryDefinition": {
            "columnClauses": [
                {"tableName": "items", "columnName": "id"},
                {"tableName": "items", "columnName": "price"}
            ],
            "whereClause": {
                "operator": "EQUAL",
                "leftOperand": {"tableName": "items", "columnName": "category"},
                "rightOperand": {"value": "book"}
            },
            "orderByClause": [
                {"columnIndex": 1, "ascending": false}
            ],
            "limitClause": {"limit": 2}
        }
    });

    let resp = server.post("/query").json(&select_query).await;
    resp.assert_status_success();

    let select_query_id: String = resp.json();
    wait_for_query_completion(&server, &select_query_id).await;

    let resp = server.get(&format!("/result/{}", select_query_id)).await;
    resp.assert_status_success();

    let result: serde_json::Value = resp.json();
    assert!(result.is_array());
    // Should return 2 rows: id 5 (price 25), id 1 (price 20)
    assert_eq!(result[0]["rowCount"], 2);

    let ids = result[0]["columns"][0].as_array().unwrap();
    let prices = result[0]["columns"][1].as_array().unwrap();
    assert_eq!(ids[0], 5);
    assert_eq!(prices[0], 25);
    assert_eq!(ids[1], 1);
    assert_eq!(prices[1], 20);
}

#[tokio::test]
async fn test_select_with_where_and_limit_only() {
    let temp_dir = TempDir::new().unwrap();
    let server = create_test_server(&temp_dir);

    // Create table
    let table_schema = serde_json::json!({
        "name": "records",
        "columns": [
            {"name": "status", "type": "VARCHAR"},
            {"name": "value", "type": "INT64"}
        ]
    });

    server.put("/table").json(&table_schema).await;

    // Create CSV
    let csv_path = temp_dir.path().join("records.csv");
    std::fs::write(
        &csv_path,
        "active,100\ninactive,200\nactive,150\ninactive,250\nactive,200\n",
    )
    .unwrap();

    // COPY data
    let copy_query = serde_json::json!({
        "queryDefinition": {
            "sourceFilepath": csv_path.to_str().unwrap(),
            "destinationTableName": "records",
            "doesCsvContainHeader": false
        }
    });

    let resp = server.post("/query").json(&copy_query).await;
    let copy_query_id: String = resp.json();
    wait_for_query_completion(&server, &copy_query_id).await;

    // SELECT active records with LIMIT 2 (no ORDER BY)
    let select_query = serde_json::json!({
        "queryDefinition": {
            "columnClauses": [
                {"tableName": "records", "columnName": "status"},
                {"tableName": "records", "columnName": "value"}
            ],
            "whereClause": {
                "operator": "EQUAL",
                "leftOperand": {"tableName": "records", "columnName": "status"},
                "rightOperand": {"value": "active"}
            },
            "limitClause": {"limit": 2}
        }
    });

    let resp = server.post("/query").json(&select_query).await;
    resp.assert_status_success();

    let select_query_id: String = resp.json();
    wait_for_query_completion(&server, &select_query_id).await;

    let resp = server.get(&format!("/result/{}", select_query_id)).await;
    resp.assert_status_success();

    let result: serde_json::Value = resp.json();
    assert!(result.is_array());
    // Should return 2 rows (first 2 "active" records)
    assert_eq!(result[0]["rowCount"], 2);
}
