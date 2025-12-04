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
            "tableName": "empty_table"
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
            "tableName": "nonexistent_table"
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
            "tableName": "employees"
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
            "tableName": "products"
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
            "tableName": "numbers"
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
                "tableName": "persistent_table"
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
            "tableName": "logs"
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
        "queryDefinition": {"tableName": "test"}
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
            "queryDefinition": {"tableName": "test"}
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
        "queryDefinition": {"tableName": "test_flush"}
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
