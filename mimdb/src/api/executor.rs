/*
 * Copyright (c) 2025-present Dawid Pawlik
 *
 * For educational use only by employees and students of MIMUW.
 * See LICENSE file for details.
 */

//! # Query Executor
//!
//! This module handles query execution, including COPY and SELECT queries.
//! It manages query lifecycle and stores results.

use crate::ColumnData;
use crate::ColumnType;
use crate::Table;
use crate::api::models::CopyQuery;
use crate::api::models::QueryDefinition;
use crate::api::models::QueryResult;
use crate::api::models::QueryResultItem;
use crate::api::models::QueryStatus;
use crate::api::models::ResultColumn;
use crate::api::models::SelectQuery;
use crate::metastore::ColumnMetadata;
use crate::metastore::Metastore;
use crate::metastore::TableMetadata;
use anyhow::Context;
use anyhow::Result;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use uuid::Uuid;

/// Plan for a COPY query
#[derive(Debug, Clone)]
pub struct CopyPlan {
    pub table_meta: TableMetadata,
    pub target_columns: Vec<ColumnMetadata>,
    pub source_filepath: String,
    pub has_header: bool,
}

/// Plan for a SELECT query
#[derive(Debug, Clone)]
pub struct SelectPlan {
    pub table_meta: TableMetadata,
    pub data_files: Vec<PathBuf>,
}

/// Query execution plan
#[derive(Debug, Clone)]
pub enum QueryPlan {
    Copy(CopyPlan),
    Select(SelectPlan),
}

/// Internal query state
#[derive(Debug, Clone)]
pub struct QueryState {
    pub query_id: String,
    pub status: QueryStatus,
    pub definition: QueryDefinition,
    pub result: Option<QueryResult>,
    pub error: Option<Vec<String>>,
}

impl QueryState {
    pub fn new(definition: QueryDefinition) -> Self {
        Self {
            query_id: Uuid::new_v4().to_string(),
            status: QueryStatus::Created,
            definition,
            result: None,
            error: None,
        }
    }
}

/// RAII guard that releases table access when dropped
struct TableAccessGuard {
    metastore: Arc<Metastore>,
    table_id: Option<String>,
    query_id: String,
}

impl Drop for TableAccessGuard {
    fn drop(&mut self) {
        if let Some(ref table_id) = self.table_id {
            self.metastore
                .release_table_access(table_id, &self.query_id);
        }
    }
}

/// Query executor manages query execution and stores results
#[derive(Debug, Clone)]
pub struct QueryExecutor {
    queries: Arc<RwLock<HashMap<String, QueryState>>>,
    metastore: Arc<Metastore>,
}

impl QueryExecutor {
    pub fn new(metastore: Arc<Metastore>) -> Self {
        Self {
            queries: Arc::new(RwLock::new(HashMap::new())),
            metastore,
        }
    }

    /// Submit a new query for execution (async - returns immediately)
    pub fn submit_query(&self, definition: QueryDefinition) -> Result<String> {
        // Validate query before submission
        self.validate_query(&definition)?;

        let state = QueryState::new(definition.clone());
        let query_id = state.query_id.clone();

        // Acquire table access before starting the query
        // This ensures files won't be deleted while the query is running
        let table_id = self.acquire_table_access_for_query(&definition, &query_id)?;

        // Store initial query state
        self.queries.write().insert(query_id.clone(), state);

        // Clone what we need for the background task
        let queries = Arc::clone(&self.queries);
        let metastore = Arc::clone(&self.metastore);
        let query_id_clone = query_id.clone();
        let table_id_for_release = table_id.clone();

        // Spawn background task for execution
        tokio::spawn(async move {
            // Ensure we release table access when done (even on error/panic)
            let _guard = TableAccessGuard {
                metastore: Arc::clone(&metastore),
                table_id: table_id_for_release,
                query_id: query_id_clone.clone(),
            };

            // Planning phase
            {
                let mut queries_guard = queries.write();
                if let Some(state) = queries_guard.get_mut(&query_id_clone) {
                    state.status = QueryStatus::Planning;
                }
            }

            // Create query plan (blocking work)
            let plan_result = tokio::task::spawn_blocking({
                let metastore = Arc::clone(&metastore);
                let definition = definition.clone();
                move || Self::plan_query(&metastore, &definition)
            })
            .await;

            // Check planning result
            let plan = match plan_result {
                Ok(Ok(plan)) => plan,
                Ok(Err(e)) => {
                    let mut queries_guard = queries.write();
                    if let Some(state) = queries_guard.get_mut(&query_id_clone) {
                        state.status = QueryStatus::Failed;
                        state.error = Some(vec![format!("Planning failed: {}", e)]);
                    }
                    return;
                }
                Err(e) => {
                    let mut queries_guard = queries.write();
                    if let Some(state) = queries_guard.get_mut(&query_id_clone) {
                        state.status = QueryStatus::Failed;
                        state.error = Some(vec![format!("Planning task panicked: {}", e)]);
                    }
                    return;
                }
            };

            // Execution phase
            {
                let mut queries_guard = queries.write();
                if let Some(state) = queries_guard.get_mut(&query_id_clone) {
                    state.status = QueryStatus::Running;
                }
            }

            // Execute the plan (blocking work)
            let result = tokio::task::spawn_blocking({
                let metastore = Arc::clone(&metastore);
                move || Self::execute_plan(&metastore, &plan)
            })
            .await;

            // Update final state
            let mut queries_guard = queries.write();
            if let Some(state) = queries_guard.get_mut(&query_id_clone) {
                match result {
                    Ok(Ok(query_result)) => {
                        state.status = QueryStatus::Completed;
                        state.result = query_result;
                    }
                    Ok(Err(e)) => {
                        state.status = QueryStatus::Failed;
                        state.error = Some(vec![format!("Execution failed: {}", e)]);
                    }
                    Err(e) => {
                        state.status = QueryStatus::Failed;
                        state.error = Some(vec![format!("Execution task panicked: {}", e)]);
                    }
                }
            }
            // _guard drops here, releasing table access
        });

        Ok(query_id)
    }

    /// Acquire table access for a query, returning the table_id
    fn acquire_table_access_for_query(
        &self,
        definition: &QueryDefinition,
        query_id: &str,
    ) -> Result<Option<String>> {
        let table_id = match definition {
            QueryDefinition::Copy(copy_query) => self
                .metastore
                .get_table_by_name(&copy_query.destination_table_name)
                .map(|t| t.table_id),
            QueryDefinition::Select(select_query) => self
                .metastore
                .get_table_by_name(&select_query.table_name)
                .map(|t| t.table_id),
        };

        if let Some(ref tid) = table_id {
            self.metastore.acquire_table_access(tid, query_id)?;
        }

        Ok(table_id)
    }

    /// Validate a query before execution
    fn validate_query(&self, definition: &QueryDefinition) -> Result<()> {
        match definition {
            QueryDefinition::Copy(copy_query) => {
                // Check if destination table exists
                if !self
                    .metastore
                    .table_exists(&copy_query.destination_table_name)
                {
                    anyhow::bail!(
                        "Table '{}' does not exist",
                        copy_query.destination_table_name
                    );
                }

                // Check if source file exists
                let path = Path::new(&copy_query.source_filepath);
                if !path.exists() {
                    anyhow::bail!(
                        "Source file '{}' does not exist",
                        copy_query.source_filepath
                    );
                }

                Ok(())
            }
            QueryDefinition::Select(select_query) => {
                // Check if table exists
                if !self.metastore.table_exists(&select_query.table_name) {
                    anyhow::bail!("Table '{}' does not exist", select_query.table_name);
                }

                Ok(())
            }
        }
    }

    /// Plan a query - prepare all metadata and validate structure
    fn plan_query(metastore: &Metastore, definition: &QueryDefinition) -> Result<QueryPlan> {
        match definition {
            QueryDefinition::Copy(copy_query) => {
                let plan = Self::plan_copy(metastore, copy_query)?;
                Ok(QueryPlan::Copy(plan))
            }
            QueryDefinition::Select(select_query) => {
                let plan = Self::plan_select(metastore, select_query)?;
                Ok(QueryPlan::Select(plan))
            }
        }
    }

    /// Plan a COPY query - resolve table metadata and column mapping
    fn plan_copy(metastore: &Metastore, query: &CopyQuery) -> Result<CopyPlan> {
        let table_meta = metastore
            .get_table_by_name(&query.destination_table_name)
            .ok_or_else(|| anyhow::anyhow!("Table '{}' not found", query.destination_table_name))?;

        // Determine column mapping
        let target_columns: Vec<ColumnMetadata> =
            if let Some(dest_cols) = &query.destination_columns {
                // Use specified columns
                dest_cols
                    .iter()
                    .map(|name| {
                        table_meta
                            .columns
                            .iter()
                            .find(|c| &c.name == name)
                            .cloned()
                            .ok_or_else(|| anyhow::anyhow!("Column '{}' not found in table", name))
                    })
                    .collect::<Result<Vec<_>>>()?
            } else {
                // Use all columns in order
                table_meta.columns.clone()
            };

        Ok(CopyPlan {
            table_meta,
            target_columns,
            source_filepath: query.source_filepath.clone(),
            has_header: query.does_csv_contain_header,
        })
    }

    /// Plan a SELECT query - resolve table metadata and list data files
    fn plan_select(metastore: &Metastore, query: &SelectQuery) -> Result<SelectPlan> {
        let table_meta = metastore
            .get_table_by_name(&query.table_name)
            .ok_or_else(|| anyhow::anyhow!("Table '{}' not found", query.table_name))?;

        // Collect all existing data files
        let data_files: Vec<PathBuf> = table_meta
            .data_files
            .iter()
            .filter(|p| p.exists())
            .cloned()
            .collect();

        Ok(SelectPlan {
            table_meta,
            data_files,
        })
    }

    /// Execute a query plan and return the result
    fn execute_plan(metastore: &Metastore, plan: &QueryPlan) -> Result<Option<QueryResult>> {
        match plan {
            QueryPlan::Copy(copy_plan) => {
                Self::execute_copy_plan(metastore, copy_plan)?;
                Ok(None) // COPY doesn't return a result
            }
            QueryPlan::Select(select_plan) => {
                let result = Self::execute_select_plan(select_plan)?;
                Ok(Some(result))
            }
        }
    }

    /// Execute a COPY query plan
    fn execute_copy_plan(metastore: &Metastore, plan: &CopyPlan) -> Result<()> {
        // Read CSV file
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(plan.has_header)
            .from_path(&plan.source_filepath)
            .context("Failed to open CSV file")?;

        // Initialize column vectors
        let mut column_data: Vec<ColumnData> = plan
            .target_columns
            .iter()
            .map(|col| match col.column_type {
                ColumnType::Int64 => ColumnData::Int64(Vec::new()),
                ColumnType::Varchar => ColumnData::Varchar(Vec::new()),
            })
            .collect();

        let expected_columns = plan.target_columns.len();

        // Read records
        for (row_idx, result) in reader.records().enumerate() {
            let record = result.context("Failed to read CSV record")?;
            let row_num = row_idx + 1 + if plan.has_header { 1 } else { 0 };

            // Validate column count
            if record.len() < expected_columns {
                anyhow::bail!(
                    "Row {}: expected {} columns, but found {} columns",
                    row_num,
                    expected_columns,
                    record.len()
                );
            }

            for (i, col_meta) in plan.target_columns.iter().enumerate() {
                let value = record.get(i).unwrap_or("");

                match &mut column_data[i] {
                    ColumnData::Int64(vec) => {
                        let trimmed = value.trim();
                        if trimmed.is_empty() {
                            anyhow::bail!(
                                "Row {}, column '{}': empty value cannot be parsed as INT64",
                                row_num,
                                col_meta.name
                            );
                        }
                        let parsed: i64 = trimmed.parse().with_context(|| {
                            format!(
                                "Row {}, column '{}': failed to parse '{}' as INT64",
                                row_num, col_meta.name, value
                            )
                        })?;
                        vec.push(parsed);
                    }
                    ColumnData::Varchar(vec) => {
                        vec.push(value.to_string());
                    }
                }
            }
        }

        // Create a new Table with the data
        let mut table = Table::new();
        for col_meta in plan.target_columns.iter() {
            table.add_column(col_meta.name.clone(), column_data.remove(0))?;
        }

        // Serialize to a new file (atomic operation)
        let data_file_path = metastore.generate_data_file_path(&plan.table_meta.table_id);

        // Ensure parent directory exists
        if let Some(parent) = data_file_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        table.serialize(&data_file_path)?;

        // Add file to metastore only after successful write
        metastore.add_data_file(&plan.table_meta.table_id, data_file_path)?;

        Ok(())
    }

    /// Execute a SELECT query plan
    fn execute_select_plan(plan: &SelectPlan) -> Result<QueryResult> {
        // Load all data files for the table
        let mut merged_columns: HashMap<String, ColumnData> = HashMap::new();
        let mut total_rows = 0usize;

        // Initialize merged columns based on table schema
        for col in &plan.table_meta.columns {
            let initial_data = match col.column_type {
                ColumnType::Int64 => ColumnData::Int64(Vec::new()),
                ColumnType::Varchar => ColumnData::Varchar(Vec::new()),
            };
            merged_columns.insert(col.name.clone(), initial_data);
        }

        // Read and merge data from all files (files were validated during planning)
        for file_path in &plan.data_files {
            let table = Table::deserialize(file_path)
                .with_context(|| format!("Failed to read data file: {:?}", file_path))?;

            for (name, data) in table.columns {
                if let Some(merged) = merged_columns.get_mut(&name) {
                    match (merged, data) {
                        (ColumnData::Int64(dest), ColumnData::Int64(src)) => {
                            dest.extend(src);
                        }
                        (ColumnData::Varchar(dest), ColumnData::Varchar(src)) => {
                            dest.extend(src);
                        }
                        _ => {}
                    }
                }
            }

            total_rows += table.row_count;
        }

        // Convert to result format, preserving column order from schema
        let mut columns = Vec::new();
        for col_meta in &plan.table_meta.columns {
            if let Some(data) = merged_columns.remove(&col_meta.name) {
                let result_col = match data {
                    ColumnData::Int64(vec) => ResultColumn::Int64(vec),
                    ColumnData::Varchar(vec) => ResultColumn::Varchar(vec),
                };
                columns.push(result_col);
            }
        }

        // QueryResult is an array of QueryResultItem as per OpenAPI spec
        Ok(vec![QueryResultItem {
            row_count: total_rows as i32,
            columns,
        }])
    }

    /// Get all queries (shallow)
    pub fn list_queries(&self) -> Vec<(String, QueryStatus)> {
        let queries = self.queries.read();
        queries
            .values()
            .map(|q| (q.query_id.clone(), q.status))
            .collect()
    }

    /// Get a specific query by ID
    pub fn get_query(&self, query_id: &str) -> Option<QueryState> {
        let queries = self.queries.read();
        queries.get(query_id).cloned()
    }

    /// Get query result
    pub fn get_result(
        &self,
        query_id: &str,
        row_limit: Option<i32>,
    ) -> Result<Option<QueryResult>> {
        let queries = self.queries.read();
        let query = queries
            .get(query_id)
            .ok_or_else(|| anyhow::anyhow!("Query not found: {}", query_id))?;

        if query.status != QueryStatus::Completed {
            anyhow::bail!("Query has not completed yet");
        }

        let result = query.result.clone();

        // Apply row limit if specified (QueryResult is Vec<QueryResultItem>)
        if let (Some(mut res), Some(limit)) = (result.clone(), row_limit) {
            // Apply limit to each result item
            for item in &mut res {
                if limit < item.row_count {
                    item.row_count = limit;
                    item.columns = item
                        .columns
                        .iter()
                        .map(|col| match col {
                            ResultColumn::Int64(vec) => ResultColumn::Int64(
                                vec.iter().take(limit as usize).cloned().collect(),
                            ),
                            ResultColumn::Varchar(vec) => ResultColumn::Varchar(
                                vec.iter().take(limit as usize).cloned().collect(),
                            ),
                        })
                        .collect();
                }
            }
            return Ok(Some(res));
        }

        Ok(result)
    }

    /// Get query error
    pub fn get_error(&self, query_id: &str) -> Result<Option<Vec<String>>> {
        let queries = self.queries.read();
        let query = queries
            .get(query_id)
            .ok_or_else(|| anyhow::anyhow!("Query not found: {}", query_id))?;

        if query.status != QueryStatus::Failed {
            anyhow::bail!("Query error is only available for failed queries");
        }

        Ok(query.error.clone())
    }

    /// Clear query result from memory
    pub fn clear_result(&self, query_id: &str) -> Result<()> {
        let mut queries = self.queries.write();
        let query = queries
            .get_mut(query_id)
            .ok_or_else(|| anyhow::anyhow!("Query not found: {}", query_id))?;

        query.result = None;
        Ok(())
    }

    /// Wait for a query to complete (for testing and synchronous use cases)
    pub async fn wait_for_completion(&self, query_id: &str) -> Result<QueryStatus> {
        loop {
            let status = {
                let queries = self.queries.read();
                queries
                    .get(query_id)
                    .map(|q| q.status)
                    .ok_or_else(|| anyhow::anyhow!("Query not found: {}", query_id))?
            };

            match status {
                QueryStatus::Completed | QueryStatus::Failed => return Ok(status),
                _ => {
                    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metastore::ColumnMetadata;
    use std::io::Write;
    use tempfile::tempdir;

    fn create_test_metastore() -> Arc<Metastore> {
        let dir = tempdir().unwrap();
        Arc::new(Metastore::new(dir.path()).unwrap())
    }

    fn create_persistent_metastore(dir: &std::path::Path) -> Arc<Metastore> {
        Arc::new(Metastore::new(dir).unwrap())
    }

    #[tokio::test]
    async fn test_select_empty_table() {
        let metastore = create_test_metastore();

        let columns = vec![
            ColumnMetadata {
                name: "id".to_string(),
                column_type: ColumnType::Int64,
            },
            ColumnMetadata {
                name: "name".to_string(),
                column_type: ColumnType::Varchar,
            },
        ];

        metastore
            .create_table("users".to_string(), columns)
            .unwrap();

        let executor = QueryExecutor::new(metastore);

        let query_def = QueryDefinition::Select(SelectQuery {
            table_name: "users".to_string(),
        });

        let query_id = executor.submit_query(query_def).unwrap();
        executor.wait_for_completion(&query_id).await.unwrap();
        let result = executor.get_result(&query_id, None).unwrap();

        assert!(result.is_some());
        let result = result.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].row_count, 0);
    }

    #[test]
    fn test_select_nonexistent_table() {
        let metastore = create_test_metastore();
        let executor = QueryExecutor::new(metastore);

        let query_def = QueryDefinition::Select(SelectQuery {
            table_name: "nonexistent".to_string(),
        });

        let result = executor.submit_query(query_def);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_copy_and_select() {
        let dir = tempdir().unwrap();
        let metastore = Arc::new(Metastore::new(dir.path()).unwrap());

        // Create table
        let columns = vec![
            ColumnMetadata {
                name: "id".to_string(),
                column_type: ColumnType::Int64,
            },
            ColumnMetadata {
                name: "name".to_string(),
                column_type: ColumnType::Varchar,
            },
        ];

        metastore
            .create_table("users".to_string(), columns)
            .unwrap();

        // Create CSV file
        let csv_path = dir.path().join("test.csv");
        let mut file = std::fs::File::create(&csv_path).unwrap();
        writeln!(file, "1,Alice").unwrap();
        writeln!(file, "2,Bob").unwrap();
        writeln!(file, "3,Charlie").unwrap();

        let executor = QueryExecutor::new(metastore);

        // Execute COPY
        let copy_def = QueryDefinition::Copy(CopyQuery {
            source_filepath: csv_path.to_str().unwrap().to_string(),
            destination_table_name: "users".to_string(),
            destination_columns: None,
            does_csv_contain_header: false,
        });

        let copy_id = executor.submit_query(copy_def).unwrap();
        executor.wait_for_completion(&copy_id).await.unwrap();
        let copy_state = executor.get_query(&copy_id).unwrap();
        assert_eq!(copy_state.status, QueryStatus::Completed);

        // Execute SELECT
        let select_def = QueryDefinition::Select(SelectQuery {
            table_name: "users".to_string(),
        });

        let select_id = executor.submit_query(select_def).unwrap();
        executor.wait_for_completion(&select_id).await.unwrap();
        let result = executor.get_result(&select_id, None).unwrap().unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].row_count, 3);
        assert_eq!(result[0].columns.len(), 2);
    }

    #[tokio::test]
    async fn test_copy_with_header() {
        let dir = tempdir().unwrap();
        let metastore = create_persistent_metastore(dir.path());

        let columns = vec![
            ColumnMetadata {
                name: "id".to_string(),
                column_type: ColumnType::Int64,
            },
            ColumnMetadata {
                name: "name".to_string(),
                column_type: ColumnType::Varchar,
            },
        ];

        metastore
            .create_table("employees".to_string(), columns)
            .unwrap();

        // CSV with header
        let csv_path = dir.path().join("employees.csv");
        let mut file = std::fs::File::create(&csv_path).unwrap();
        writeln!(file, "id,name").unwrap();
        writeln!(file, "100,John").unwrap();
        writeln!(file, "200,Jane").unwrap();

        let executor = QueryExecutor::new(metastore);

        let copy_def = QueryDefinition::Copy(CopyQuery {
            source_filepath: csv_path.to_str().unwrap().to_string(),
            destination_table_name: "employees".to_string(),
            destination_columns: None,
            does_csv_contain_header: true,
        });

        let copy_id = executor.submit_query(copy_def).unwrap();
        executor.wait_for_completion(&copy_id).await.unwrap();
        let query = executor.get_query(&copy_id).unwrap();
        assert_eq!(query.status, QueryStatus::Completed);

        // Select and verify
        let select_def = QueryDefinition::Select(SelectQuery {
            table_name: "employees".to_string(),
        });
        let select_id = executor.submit_query(select_def).unwrap();
        executor.wait_for_completion(&select_id).await.unwrap();
        let result = executor.get_result(&select_id, None).unwrap().unwrap();

        assert_eq!(result[0].row_count, 2); // Header should be skipped
    }

    #[tokio::test]
    async fn test_copy_with_specific_columns() {
        let dir = tempdir().unwrap();
        let metastore = create_persistent_metastore(dir.path());

        let columns = vec![
            ColumnMetadata {
                name: "id".to_string(),
                column_type: ColumnType::Int64,
            },
            ColumnMetadata {
                name: "name".to_string(),
                column_type: ColumnType::Varchar,
            },
            ColumnMetadata {
                name: "age".to_string(),
                column_type: ColumnType::Int64,
            },
        ];

        metastore
            .create_table("persons".to_string(), columns)
            .unwrap();

        // CSV with 2 columns (id, name)
        let csv_path = dir.path().join("persons.csv");
        let mut file = std::fs::File::create(&csv_path).unwrap();
        writeln!(file, "1,Alice").unwrap();
        writeln!(file, "2,Bob").unwrap();

        let executor = QueryExecutor::new(metastore);

        let copy_def = QueryDefinition::Copy(CopyQuery {
            source_filepath: csv_path.to_str().unwrap().to_string(),
            destination_table_name: "persons".to_string(),
            destination_columns: Some(vec!["id".to_string(), "name".to_string()]),
            does_csv_contain_header: false,
        });

        let copy_id = executor.submit_query(copy_def).unwrap();
        executor.wait_for_completion(&copy_id).await.unwrap();
        let query = executor.get_query(&copy_id).unwrap();
        assert_eq!(query.status, QueryStatus::Completed);
    }

    #[tokio::test]
    async fn test_multiple_copy_operations() {
        let dir = tempdir().unwrap();
        let metastore = create_persistent_metastore(dir.path());

        let columns = vec![ColumnMetadata {
            name: "value".to_string(),
            column_type: ColumnType::Int64,
        }];

        metastore
            .create_table("numbers".to_string(), columns)
            .unwrap();

        let executor = QueryExecutor::new(metastore);

        // First COPY
        let csv1_path = dir.path().join("numbers1.csv");
        let mut file1 = std::fs::File::create(&csv1_path).unwrap();
        writeln!(file1, "1").unwrap();
        writeln!(file1, "2").unwrap();

        let copy1_def = QueryDefinition::Copy(CopyQuery {
            source_filepath: csv1_path.to_str().unwrap().to_string(),
            destination_table_name: "numbers".to_string(),
            destination_columns: None,
            does_csv_contain_header: false,
        });
        let copy1_id = executor.submit_query(copy1_def).unwrap();
        executor.wait_for_completion(&copy1_id).await.unwrap();

        // Second COPY
        let csv2_path = dir.path().join("numbers2.csv");
        let mut file2 = std::fs::File::create(&csv2_path).unwrap();
        writeln!(file2, "3").unwrap();
        writeln!(file2, "4").unwrap();
        writeln!(file2, "5").unwrap();

        let copy2_def = QueryDefinition::Copy(CopyQuery {
            source_filepath: csv2_path.to_str().unwrap().to_string(),
            destination_table_name: "numbers".to_string(),
            destination_columns: None,
            does_csv_contain_header: false,
        });
        let copy2_id = executor.submit_query(copy2_def).unwrap();
        executor.wait_for_completion(&copy2_id).await.unwrap();

        // SELECT should return all rows from both COPY operations
        let select_def = QueryDefinition::Select(SelectQuery {
            table_name: "numbers".to_string(),
        });
        let select_id = executor.submit_query(select_def).unwrap();
        executor.wait_for_completion(&select_id).await.unwrap();
        let result = executor.get_result(&select_id, None).unwrap().unwrap();

        assert_eq!(result[0].row_count, 5);
    }

    #[tokio::test]
    async fn test_get_result_with_row_limit() {
        let dir = tempdir().unwrap();
        let metastore = create_persistent_metastore(dir.path());

        let columns = vec![ColumnMetadata {
            name: "id".to_string(),
            column_type: ColumnType::Int64,
        }];

        metastore.create_table("data".to_string(), columns).unwrap();

        // Create CSV with 10 rows
        let csv_path = dir.path().join("data.csv");
        let mut file = std::fs::File::create(&csv_path).unwrap();
        for i in 1..=10 {
            writeln!(file, "{}", i).unwrap();
        }

        let executor = QueryExecutor::new(metastore);

        // COPY
        let copy_def = QueryDefinition::Copy(CopyQuery {
            source_filepath: csv_path.to_str().unwrap().to_string(),
            destination_table_name: "data".to_string(),
            destination_columns: None,
            does_csv_contain_header: false,
        });
        let copy_id = executor.submit_query(copy_def).unwrap();
        executor.wait_for_completion(&copy_id).await.unwrap();

        // SELECT
        let select_def = QueryDefinition::Select(SelectQuery {
            table_name: "data".to_string(),
        });
        let select_id = executor.submit_query(select_def).unwrap();
        executor.wait_for_completion(&select_id).await.unwrap();

        // Get result with limit less than row count
        let result = executor.get_result(&select_id, Some(3)).unwrap().unwrap();
        assert_eq!(result[0].row_count, 3);

        // Get result without limit
        let full_result = executor.get_result(&select_id, None).unwrap().unwrap();
        assert_eq!(full_result[0].row_count, 10);

        // Get result with limit greater than row count - should return all rows
        let result_high_limit = executor.get_result(&select_id, Some(100)).unwrap().unwrap();
        assert_eq!(result_high_limit[0].row_count, 10);

        // Get result with limit equal to row count
        let result_exact_limit = executor.get_result(&select_id, Some(10)).unwrap().unwrap();
        assert_eq!(result_exact_limit[0].row_count, 10);
    }

    #[tokio::test]
    async fn test_list_queries() {
        let metastore = create_test_metastore();

        let columns = vec![ColumnMetadata {
            name: "id".to_string(),
            column_type: ColumnType::Int64,
        }];
        metastore.create_table("test".to_string(), columns).unwrap();

        let executor = QueryExecutor::new(metastore);

        // Submit multiple queries
        let select1 = QueryDefinition::Select(SelectQuery {
            table_name: "test".to_string(),
        });
        let select2 = QueryDefinition::Select(SelectQuery {
            table_name: "test".to_string(),
        });

        let id1 = executor.submit_query(select1).unwrap();
        let id2 = executor.submit_query(select2).unwrap();
        executor.wait_for_completion(&id1).await.unwrap();
        executor.wait_for_completion(&id2).await.unwrap();

        let queries = executor.list_queries();
        assert_eq!(queries.len(), 2);

        // All should be completed
        for (_id, status) in queries {
            assert_eq!(status, QueryStatus::Completed);
        }
    }

    #[test]
    fn test_get_nonexistent_query() {
        let metastore = create_test_metastore();
        let executor = QueryExecutor::new(metastore);

        assert!(executor.get_query("nonexistent-query-id").is_none());
    }

    #[test]
    fn test_get_result_nonexistent_query() {
        let metastore = create_test_metastore();
        let executor = QueryExecutor::new(metastore);

        let result = executor.get_result("nonexistent-query-id", None);
        assert!(result.is_err());
    }

    #[test]
    fn test_copy_missing_file() {
        let dir = tempdir().unwrap();
        let metastore = create_persistent_metastore(dir.path());

        let columns = vec![ColumnMetadata {
            name: "id".to_string(),
            column_type: ColumnType::Int64,
        }];
        metastore.create_table("test".to_string(), columns).unwrap();

        let executor = QueryExecutor::new(metastore);

        let copy_def = QueryDefinition::Copy(CopyQuery {
            source_filepath: "/nonexistent/path/file.csv".to_string(),
            destination_table_name: "test".to_string(),
            destination_columns: None,
            does_csv_contain_header: false,
        });

        let result = executor.submit_query(copy_def);
        // Should fail because file doesn't exist
        assert!(result.is_err());
    }

    #[test]
    fn test_copy_to_nonexistent_table() {
        let metastore = create_test_metastore();
        let executor = QueryExecutor::new(metastore);

        let copy_def = QueryDefinition::Copy(CopyQuery {
            source_filepath: "/some/file.csv".to_string(),
            destination_table_name: "nonexistent".to_string(),
            destination_columns: None,
            does_csv_contain_header: false,
        });

        let result = executor.submit_query(copy_def);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_varchar_data_handling() {
        let dir = tempdir().unwrap();
        let metastore = create_persistent_metastore(dir.path());

        let columns = vec![
            ColumnMetadata {
                name: "id".to_string(),
                column_type: ColumnType::Int64,
            },
            ColumnMetadata {
                name: "text".to_string(),
                column_type: ColumnType::Varchar,
            },
        ];

        metastore
            .create_table("strings".to_string(), columns)
            .unwrap();

        let csv_path = dir.path().join("strings.csv");
        let mut file = std::fs::File::create(&csv_path).unwrap();
        writeln!(file, "1,Hello World").unwrap();
        writeln!(file, "2,Special chars: äöü").unwrap();
        writeln!(file, "3,").unwrap(); // empty string

        let executor = QueryExecutor::new(metastore);

        let copy_def = QueryDefinition::Copy(CopyQuery {
            source_filepath: csv_path.to_str().unwrap().to_string(),
            destination_table_name: "strings".to_string(),
            destination_columns: None,
            does_csv_contain_header: false,
        });
        let copy_id = executor.submit_query(copy_def).unwrap();
        executor.wait_for_completion(&copy_id).await.unwrap();

        let select_def = QueryDefinition::Select(SelectQuery {
            table_name: "strings".to_string(),
        });
        let select_id = executor.submit_query(select_def).unwrap();
        executor.wait_for_completion(&select_id).await.unwrap();
        let result = executor.get_result(&select_id, None).unwrap().unwrap();

        assert_eq!(result[0].row_count, 3);

        // Verify varchar column data
        match &result[0].columns[1] {
            ResultColumn::Varchar(vec) => {
                assert_eq!(vec.len(), 3);
                assert_eq!(vec[0], "Hello World");
            }
            _ => panic!("Expected varchar column"),
        }
    }

    #[tokio::test]
    async fn test_query_status_tracking() {
        let metastore = create_test_metastore();

        let columns = vec![ColumnMetadata {
            name: "id".to_string(),
            column_type: ColumnType::Int64,
        }];
        metastore.create_table("test".to_string(), columns).unwrap();

        let executor = QueryExecutor::new(metastore);

        let select_def = QueryDefinition::Select(SelectQuery {
            table_name: "test".to_string(),
        });
        let query_id = executor.submit_query(select_def).unwrap();
        executor.wait_for_completion(&query_id).await.unwrap();

        let query = executor.get_query(&query_id).unwrap();
        assert_eq!(query.status, QueryStatus::Completed);
        assert!(query.error.is_none());
        assert!(query.result.is_some());
    }

    #[tokio::test]
    async fn test_copy_with_empty_int64_cell() {
        let dir = tempdir().unwrap();
        let metastore = create_persistent_metastore(dir.path());

        let columns = vec![
            ColumnMetadata {
                name: "id".to_string(),
                column_type: ColumnType::Int64,
            },
            ColumnMetadata {
                name: "value".to_string(),
                column_type: ColumnType::Int64,
            },
        ];
        metastore.create_table("test".to_string(), columns).unwrap();

        // CSV with empty cell in INT64 column
        let csv_path = dir.path().join("empty_int.csv");
        let mut file = std::fs::File::create(&csv_path).unwrap();
        writeln!(file, "1,100").unwrap();
        writeln!(file, "2,").unwrap(); // Empty INT64 value
        writeln!(file, "3,300").unwrap();

        let executor = QueryExecutor::new(metastore);

        let copy_def = QueryDefinition::Copy(CopyQuery {
            source_filepath: csv_path.to_str().unwrap().to_string(),
            destination_table_name: "test".to_string(),
            destination_columns: None,
            does_csv_contain_header: false,
        });
        let query_id = executor.submit_query(copy_def).unwrap();
        executor.wait_for_completion(&query_id).await.unwrap();

        // Query should fail due to empty INT64 value
        let query = executor.get_query(&query_id).unwrap();
        assert_eq!(query.status, QueryStatus::Failed);
        assert!(query.error.is_some());
        let error_msg = query.error.unwrap().join(" ");
        assert!(error_msg.contains("empty value"));
        assert!(error_msg.contains("INT64"));
    }

    #[tokio::test]
    async fn test_copy_with_missing_columns() {
        let dir = tempdir().unwrap();
        let metastore = create_persistent_metastore(dir.path());

        let columns = vec![
            ColumnMetadata {
                name: "id".to_string(),
                column_type: ColumnType::Int64,
            },
            ColumnMetadata {
                name: "name".to_string(),
                column_type: ColumnType::Varchar,
            },
            ColumnMetadata {
                name: "value".to_string(),
                column_type: ColumnType::Int64,
            },
        ];
        metastore.create_table("test".to_string(), columns).unwrap();

        // CSV with fewer columns than expected
        let csv_path = dir.path().join("missing_cols.csv");
        let mut file = std::fs::File::create(&csv_path).unwrap();
        writeln!(file, "1,Alice,100").unwrap();
        writeln!(file, "2,Bob").unwrap(); // Missing third column
        writeln!(file, "3,Charlie,300").unwrap();

        let executor = QueryExecutor::new(metastore);

        let copy_def = QueryDefinition::Copy(CopyQuery {
            source_filepath: csv_path.to_str().unwrap().to_string(),
            destination_table_name: "test".to_string(),
            destination_columns: None,
            does_csv_contain_header: false,
        });
        let query_id = executor.submit_query(copy_def).unwrap();
        executor.wait_for_completion(&query_id).await.unwrap();

        // Query should fail due to column count mismatch (CSV parser enforces strict mode)
        let query = executor.get_query(&query_id).unwrap();
        assert_eq!(query.status, QueryStatus::Failed);
        assert!(query.error.is_some());
    }

    #[tokio::test]
    async fn test_copy_with_invalid_int64_value() {
        let dir = tempdir().unwrap();
        let metastore = create_persistent_metastore(dir.path());

        let columns = vec![ColumnMetadata {
            name: "id".to_string(),
            column_type: ColumnType::Int64,
        }];
        metastore.create_table("test".to_string(), columns).unwrap();

        // CSV with non-numeric value in INT64 column
        let csv_path = dir.path().join("invalid_int.csv");
        let mut file = std::fs::File::create(&csv_path).unwrap();
        writeln!(file, "1").unwrap();
        writeln!(file, "abc").unwrap(); // Invalid INT64 value
        writeln!(file, "3").unwrap();

        let executor = QueryExecutor::new(metastore);

        let copy_def = QueryDefinition::Copy(CopyQuery {
            source_filepath: csv_path.to_str().unwrap().to_string(),
            destination_table_name: "test".to_string(),
            destination_columns: None,
            does_csv_contain_header: false,
        });
        let query_id = executor.submit_query(copy_def).unwrap();
        executor.wait_for_completion(&query_id).await.unwrap();

        // Query should fail due to invalid INT64 value
        let query = executor.get_query(&query_id).unwrap();
        assert_eq!(query.status, QueryStatus::Failed);
        assert!(query.error.is_some());
        let error_msg = query.error.unwrap().join(" ");
        assert!(error_msg.contains("failed to parse"));
        assert!(error_msg.contains("abc"));
    }

    #[tokio::test]
    async fn test_copy_with_extra_columns_ok() {
        let dir = tempdir().unwrap();
        let metastore = create_persistent_metastore(dir.path());

        let columns = vec![
            ColumnMetadata {
                name: "id".to_string(),
                column_type: ColumnType::Int64,
            },
            ColumnMetadata {
                name: "name".to_string(),
                column_type: ColumnType::Varchar,
            },
        ];
        metastore.create_table("test".to_string(), columns).unwrap();

        // CSV with more columns than the table expects - extra columns ignored
        let csv_path = dir.path().join("extra_cols.csv");
        let mut file = std::fs::File::create(&csv_path).unwrap();
        writeln!(file, "1,Alice,extra1,extra2").unwrap();
        writeln!(file, "2,Bob,extra3,extra4").unwrap();

        let executor = QueryExecutor::new(metastore);

        let copy_def = QueryDefinition::Copy(CopyQuery {
            source_filepath: csv_path.to_str().unwrap().to_string(),
            destination_table_name: "test".to_string(),
            destination_columns: None,
            does_csv_contain_header: false,
        });
        let query_id = executor.submit_query(copy_def).unwrap();
        executor.wait_for_completion(&query_id).await.unwrap();

        // Query should succeed - extra columns are ignored
        let query = executor.get_query(&query_id).unwrap();
        assert_eq!(query.status, QueryStatus::Completed);

        // Verify data was loaded correctly
        let select_def = QueryDefinition::Select(SelectQuery {
            table_name: "test".to_string(),
        });
        let select_id = executor.submit_query(select_def).unwrap();
        executor.wait_for_completion(&select_id).await.unwrap();
        let result = executor.get_result(&select_id, None).unwrap().unwrap();

        assert_eq!(result[0].row_count, 2);
    }
}
