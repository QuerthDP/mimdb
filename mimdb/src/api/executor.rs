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
use crate::api::expression::CseContext;
use crate::api::expression::EvaluationContext;
use crate::api::expression::ExpressionEvaluator;
use crate::api::expression::collect_column_references;
use crate::api::expression::collect_subexpressions;
use crate::api::expression::collect_table_references;
use crate::api::models::ColumnExpression;
use crate::api::models::ColumnExpression::BinaryOperation;
use crate::api::models::ColumnExpression::ColumnReference;
use crate::api::models::ColumnExpression::Function;
use crate::api::models::ColumnExpression::Literal;
use crate::api::models::ColumnExpression::UnaryOperation;
use crate::api::models::CopyQuery;
use crate::api::models::LimitExpression;
use crate::api::models::OrderByExpression;
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
use std::collections::HashSet;
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
    /// Table metadata for the referenced table
    pub table_meta: TableMetadata,
    /// Data files to read
    pub data_files: Vec<PathBuf>,
    /// Column metadata for only the columns required by the query
    pub required_columns: Vec<ColumnMetadata>,
    /// Column expressions to evaluate (output columns)
    pub column_clauses: Vec<ColumnExpression>,
    /// Optional WHERE clause for filtering
    pub where_clause: Option<ColumnExpression>,
    /// Optional ORDER BY clause
    pub order_by_clause: Option<Vec<OrderByExpression>>,
    /// Optional LIMIT clause
    pub limit_clause: Option<LimitExpression>,
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

/// Extract the single table name referenced in a SELECT query.
/// If explicit table names are provided, use them.
/// If no explicit table names, this function collects column names from expressions
/// and uses the provided metastore to find a matching table.
/// If no column references exist use any existing table.
fn extract_single_table_reference(
    select_query: &SelectQuery,
    metastore: &Metastore,
) -> Result<String> {
    let mut table_names = HashSet::new();
    for expr in &select_query.column_clauses {
        table_names.extend(collect_table_references(expr));
    }
    if let Some(ref where_clause) = select_query.where_clause {
        table_names.extend(collect_table_references(where_clause));
    }

    if table_names.len() > 1 {
        anyhow::bail!(
            "SELECT query references multiple tables ({:?}), but only single-table queries are supported",
            table_names
        );
    }

    if let Some(table_name) = table_names.into_iter().next() {
        return Ok(table_name);
    }

    // No explicit table names found - try to infer from column names
    let mut column_names = HashSet::new();
    for expr in &select_query.column_clauses {
        column_names.extend(collect_column_references(expr));
    }
    if let Some(ref where_clause) = select_query.where_clause {
        column_names.extend(collect_column_references(where_clause));
    }

    // If no column references, use any available table
    if column_names.is_empty() {
        let tables = metastore.list_tables();
        return match tables.into_iter().next() {
            Some((_, table_name)) => Ok(table_name),
            None => anyhow::bail!("No tables exist in the database"),
        };
    }

    // Find tables that have all the referenced columns
    let tables = metastore.list_tables();
    let mut matching_tables = Vec::new();

    for (table_id, _table_name) in &tables {
        if let Some(table_meta) = metastore.get_table(table_id) {
            let table_columns: HashSet<String> =
                table_meta.columns.iter().map(|c| c.name.clone()).collect();
            if column_names.iter().all(|c| table_columns.contains(c)) {
                matching_tables.push(table_meta.name.clone());
            }
        }
    }

    match matching_tables.len() {
        0 => anyhow::bail!("No table found containing columns: {:?}", column_names),
        1 => Ok(matching_tables.into_iter().next().unwrap()),
        _ => anyhow::bail!(
            "Ambiguous column references: multiple tables ({:?}) contain the referenced columns. Please specify table names explicitly.",
            matching_tables
        ),
    }
}

impl QueryExecutor {
    pub fn new(metastore: Arc<Metastore>) -> Self {
        Self {
            queries: Arc::new(RwLock::new(HashMap::new())),
            metastore,
        }
    }

    /// Submit a new query for execution
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
            QueryDefinition::Select(select_query) => {
                extract_single_table_reference(select_query, &self.metastore)
                    .ok()
                    .and_then(|name| self.metastore.get_table_by_name(&name))
                    .map(|t| t.table_id)
            }
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
                // Validate that column clauses is not empty
                if select_query.column_clauses.is_empty() {
                    anyhow::bail!("SELECT query must have at least one column clause");
                }

                // Extract and validate the single table reference
                let table_name = extract_single_table_reference(select_query, &self.metastore)?;

                // Check that the table exists
                if !self.metastore.table_exists(&table_name) {
                    anyhow::bail!("Table '{}' does not exist", table_name);
                }

                // Validate order by indices
                if let Some(ref order_by) = select_query.order_by_clause {
                    for order in order_by {
                        if order.column_index >= select_query.column_clauses.len() {
                            anyhow::bail!(
                                "ORDER BY column index {} is out of bounds (max: {})",
                                order.column_index,
                                select_query.column_clauses.len() - 1
                            );
                        }
                    }
                }

                // Validate limit is positive
                if let Some(ref limit) = select_query.limit_clause
                    && limit.limit < 0
                {
                    anyhow::bail!("LIMIT must be non-negative, got {}", limit.limit);
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
        let table_name = extract_single_table_reference(query, metastore)?;
        let table_meta = metastore
            .get_table_by_name(&table_name)
            .ok_or_else(|| anyhow::anyhow!("Table '{}' not found", table_name))?;

        // Collect all existing data files
        let data_files: Vec<PathBuf> = table_meta
            .data_files
            .iter()
            .filter(|p| p.exists())
            .cloned()
            .collect();

        let column_types = Self::build_column_type_map(&table_meta);

        // Validate column expressions
        for (i, expr) in query.column_clauses.iter().enumerate() {
            Self::validate_expression(expr, &column_types, Some(&table_name))
                .context(format!("Invalid column expression at index {}", i))?;
        }

        // Validate WHERE clause expression
        if let Some(ref where_clause) = query.where_clause {
            Self::validate_expression(where_clause, &column_types, Some(&table_name))
                .context("Invalid WHERE clause expression")?;

            // WHERE clause must evaluate to boolean
            let where_type =
                ExpressionEvaluator::infer_type(where_clause, &column_types, Some(&table_name))?;
            if where_type != ColumnType::Bool {
                anyhow::bail!("WHERE clause must evaluate to BOOL, got {:?}", where_type);
            }
        }

        // Collect all column names referenced in the query
        let mut required_column_names: HashSet<String> = HashSet::new();
        for expr in &query.column_clauses {
            required_column_names.extend(collect_column_references(expr));
        }
        if let Some(ref where_clause) = query.where_clause {
            required_column_names.extend(collect_column_references(where_clause));
        }

        // Build required columns metadata (preserving order from table schema)
        let required_columns: Vec<ColumnMetadata> = table_meta
            .columns
            .iter()
            .filter(|col| required_column_names.contains(&col.name))
            .cloned()
            .collect();

        Ok(SelectPlan {
            table_meta,
            data_files,
            required_columns,
            column_clauses: query.column_clauses.clone(),
            where_clause: query.where_clause.clone(),
            order_by_clause: query.order_by_clause.clone(),
            limit_clause: query.limit_clause.clone(),
        })
    }

    /// Build a map of (table_name, column_name) -> ColumnType
    fn build_column_type_map(table_meta: &TableMetadata) -> HashMap<(String, String), ColumnType> {
        let mut column_types = HashMap::new();
        for col in &table_meta.columns {
            column_types.insert(
                (table_meta.name.clone(), col.name.clone()),
                col.column_type.clone(),
            );
        }
        column_types
    }

    /// Validate an expression - check that all column references exist
    fn validate_expression(
        expr: &ColumnExpression,
        column_types: &HashMap<(String, String), ColumnType>,
        default_table: Option<&str>,
    ) -> Result<()> {
        match expr {
            ColumnReference(col_ref) => {
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
                if !column_types.contains_key(&key) {
                    anyhow::bail!("Column '{}.{}' not found", table_name, col_ref.column_name);
                }
                Ok(())
            }
            Literal(_) => Ok(()),
            Function(func) => {
                for arg in &func.arguments {
                    Self::validate_expression(arg, column_types, default_table)?;
                }
                Ok(())
            }
            BinaryOperation(bin_op) => {
                Self::validate_expression(&bin_op.left_operand, column_types, default_table)?;
                Self::validate_expression(&bin_op.right_operand, column_types, default_table)?;
                Ok(())
            }
            UnaryOperation(unary_op) => {
                Self::validate_expression(&unary_op.operand, column_types, default_table)?;
                Ok(())
            }
        }
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
                ColumnType::Bool => ColumnData::Bool(Vec::new()),
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
                    ColumnData::Bool(vec) => {
                        let trimmed = value.trim().to_lowercase();
                        let parsed = match trimmed.as_str() {
                            "true" | "1" | "yes" | "t" | "y" => true,
                            "false" | "0" | "no" | "f" | "n" | "" => false,
                            _ => anyhow::bail!(
                                "Row {}, column '{}': failed to parse '{}' as BOOL",
                                row_num,
                                col_meta.name,
                                value
                            ),
                        };
                        vec.push(parsed);
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
        let mut columns: HashMap<String, ColumnData> = HashMap::new();
        for col in &plan.required_columns {
            let initial_data = match col.column_type {
                ColumnType::Int64 => ColumnData::Int64(Vec::new()),
                ColumnType::Varchar => ColumnData::Varchar(Vec::new()),
                ColumnType::Bool => ColumnData::Bool(Vec::new()),
            };
            columns.insert(col.name.clone(), initial_data);
        }

        let mut total_rows = 0usize;
        for file_path in &plan.data_files {
            let table = Table::deserialize(file_path)
                .context(format!("Failed to read data file: {:?}", file_path))?;

            for (name, data) in table.columns {
                if let Some(col_storage) = columns.get_mut(&name) {
                    match (col_storage, data) {
                        (ColumnData::Int64(dest), ColumnData::Int64(src)) => {
                            dest.extend(src);
                        }
                        (ColumnData::Varchar(dest), ColumnData::Varchar(src)) => {
                            dest.extend(src);
                        }
                        (ColumnData::Bool(dest), ColumnData::Bool(src)) => {
                            dest.extend(src);
                        }
                        _ => {}
                    }
                }
            }

            total_rows += table.row_count;
        }

        // If no ORDER BY clause, we may evaluate only up to LIMIT rows early
        let early_limit = if plan.order_by_clause.is_none() {
            plan.limit_clause.as_ref().map(|l| l.limit as usize)
        } else {
            None
        };

        // Apply WHERE clause and evaluate column expressions
        let result_columns = Self::filter_and_evaluate(
            &plan.table_meta.name,
            columns,
            total_rows,
            plan.where_clause.as_ref(),
            &plan.column_clauses,
            early_limit,
        )?;

        let filtered_row_count = result_columns.first().map(|c| c.len()).unwrap_or(0);

        // Apply ORDER BY if specified
        let ordered_indices: Vec<usize> = if let Some(ref order_by) = plan.order_by_clause {
            Self::compute_sort_order(&result_columns, order_by, filtered_row_count)?
        } else {
            (0..filtered_row_count).collect()
        };

        // Apply LIMIT if specified
        let final_indices: Vec<usize> = if let Some(ref limit) = plan.limit_clause {
            let limit_count = limit.limit as usize;
            ordered_indices.into_iter().take(limit_count).collect()
        } else {
            ordered_indices
        };

        // Return empty result if no rows
        if final_indices.is_empty() {
            return Ok(vec![]);
        }

        // Reorder result columns based on final indices
        let final_columns: Vec<ResultColumn> = result_columns
            .into_iter()
            .map(|col| Self::reorder_column_data(col, &final_indices))
            .collect();

        Ok(vec![QueryResultItem {
            row_count: final_indices.len() as i32,
            columns: final_columns,
        }])
    }

    /// Filter rows based on WHERE clause and evaluate column expressions
    /// Uses Common Subexpression Elimination (CSE) to avoid redundant computation
    /// of expressions that appear in both WHERE and SELECT clauses.
    ///
    /// The CSE optimization works as follows:
    /// 1. Find expressions that are common between WHERE and SELECT
    /// 2. Pre-compute these common expressions before filtering (input projection)
    /// 3. Use the pre-computed values during filtering
    /// 4. After filtering, reuse the pre-computed values for SELECT evaluation
    fn filter_and_evaluate(
        table_name: &str,
        columns: HashMap<String, ColumnData>,
        row_count: usize,
        where_clause: Option<&ColumnExpression>,
        column_clauses: &[ColumnExpression],
        early_limit: Option<usize>,
    ) -> Result<Vec<ColumnData>> {
        // Build base context for expression evaluation
        let mut base_column_map = HashMap::new();
        for (col_name, data) in &columns {
            base_column_map.insert((table_name.to_string(), col_name.clone()), data);
        }

        let common_expr_hashes: HashSet<u64> = if let Some(where_expr) = where_clause {
            // Collect subexpressions from WHERE
            let where_subexprs = collect_subexpressions(where_expr);
            let where_hashes: HashSet<u64> = where_subexprs.keys().cloned().collect();

            // Collect subexpressions from all SELECT clauses
            let mut select_hashes: HashSet<u64> = HashSet::new();
            for expr in column_clauses {
                let subexprs = collect_subexpressions(expr);
                select_hashes.extend(subexprs.keys().cloned());
            }

            where_hashes.intersection(&select_hashes).cloned().collect()
        } else {
            HashSet::new()
        };

        let mut pre_computed: HashMap<u64, ColumnData> = HashMap::new();

        if !common_expr_hashes.is_empty()
            && let Some(where_expr) = where_clause
        {
            let ctx = EvaluationContext::with_default_table(
                base_column_map.clone(),
                row_count,
                table_name.to_string(),
            );
            let mut cse = CseContext::new();

            // Evaluate WHERE expression with CSE - this will populate the cache
            // with all subexpressions including the common ones
            let _ = ExpressionEvaluator::evaluate(where_expr, &ctx, &mut cse)?;

            // Extract the common subexpressions from the cache
            for hash in &common_expr_hashes {
                if let Some(data) = cse.computed.get(hash) {
                    pre_computed.insert(*hash, data.clone());
                }
            }
        }

        let (final_columns, final_row_count, passing_indices) =
            if let Some(where_expr) = where_clause {
                let ctx = EvaluationContext::with_default_table(
                    base_column_map.clone(),
                    row_count,
                    table_name.to_string(),
                );

                // Create CSE context pre-populated with already computed expressions
                let mut cse = CseContext::new();
                for (hash, data) in &pre_computed {
                    cse.computed.insert(*hash, data.clone());
                }

                // Evaluate WHERE clause to get filter mask
                let filter_result = ExpressionEvaluator::evaluate(where_expr, &ctx, &mut cse)?;
                let indices: Vec<usize> = match filter_result {
                    ColumnData::Bool(mask) => {
                        let iter = mask
                            .into_iter()
                            .enumerate()
                            .filter_map(|(i, passes)| if passes { Some(i) } else { None });
                        if let Some(limit) = early_limit {
                            iter.take(limit).collect()
                        } else {
                            iter.collect()
                        }
                    }
                    _ => anyhow::bail!("WHERE clause must evaluate to BOOL"),
                };

                let filtered: HashMap<String, ColumnData> = columns
                    .into_iter()
                    .map(|(name, data)| (name, Self::filter_column_data(data, &indices)))
                    .collect();
                let len = indices.len();
                (filtered, len, Some(indices))
            } else if let Some(limit) = early_limit
                && limit < row_count
            {
                let limited: HashMap<String, ColumnData> = columns
                    .into_iter()
                    .map(|(name, data)| (name, Self::take_first_n(data, limit)))
                    .collect();
                (limited, limit, None)
            } else {
                (columns, row_count, None)
            };

        // Build final context and evaluate column expressions
        let mut column_map = HashMap::new();
        for (col_name, data) in &final_columns {
            column_map.insert((table_name.to_string(), col_name.clone()), data);
        }
        let ctx = EvaluationContext::with_default_table(
            column_map,
            final_row_count,
            table_name.to_string(),
        );

        let mut cse = CseContext::new();

        // If we have pre-computed expressions and we filtered rows, we need to
        // filter the pre-computed data as well
        if let Some(ref indices) = passing_indices {
            for (hash, data) in pre_computed {
                let filtered_data = Self::filter_column_data(data, indices);
                cse.computed.insert(hash, filtered_data);
            }
        } else if !pre_computed.is_empty() {
            // No filtering happened, use pre-computed data as-is
            if early_limit.is_some() && final_row_count < row_count {
                for (hash, data) in pre_computed {
                    let limited_data = Self::take_first_n(data, final_row_count);
                    cse.computed.insert(hash, limited_data);
                }
            } else {
                cse.computed = pre_computed;
            }
        }

        column_clauses
            .iter()
            .map(|expr| ExpressionEvaluator::evaluate(expr, &ctx, &mut cse))
            .collect()
    }

    /// Filter column data to only include rows at the specified indices
    fn filter_column_data(data: ColumnData, indices: &[usize]) -> ColumnData {
        match data {
            ColumnData::Int64(vec) => ColumnData::Int64(indices.iter().map(|&i| vec[i]).collect()),
            ColumnData::Varchar(vec) => {
                ColumnData::Varchar(indices.iter().map(|&i| vec[i].clone()).collect())
            }
            ColumnData::Bool(vec) => ColumnData::Bool(indices.iter().map(|&i| vec[i]).collect()),
        }
    }

    /// Take the first n elements from column data
    fn take_first_n(data: ColumnData, n: usize) -> ColumnData {
        match data {
            ColumnData::Int64(vec) => ColumnData::Int64(vec.into_iter().take(n).collect()),
            ColumnData::Varchar(vec) => ColumnData::Varchar(vec.into_iter().take(n).collect()),
            ColumnData::Bool(vec) => ColumnData::Bool(vec.into_iter().take(n).collect()),
        }
    }

    /// Compute sort order based on ORDER BY clause
    fn compute_sort_order(
        columns: &[ColumnData],
        order_by: &[OrderByExpression],
        row_count: usize,
    ) -> Result<Vec<usize>> {
        let mut indices: Vec<usize> = (0..row_count).collect();

        indices.sort_by(|&a, &b| {
            for order in order_by {
                let col_idx = order.column_index;
                assert!(col_idx < columns.len());

                let cmp = match &columns[col_idx] {
                    ColumnData::Int64(vec) => vec[a].cmp(&vec[b]),
                    ColumnData::Varchar(vec) => vec[a].cmp(&vec[b]),
                    ColumnData::Bool(vec) => vec[a].cmp(&vec[b]),
                };

                let cmp = if order.ascending { cmp } else { cmp.reverse() };

                if cmp != std::cmp::Ordering::Equal {
                    return cmp;
                }
            }
            std::cmp::Ordering::Equal
        });

        Ok(indices)
    }

    /// Reorder column data based on indices
    fn reorder_column_data(data: ColumnData, indices: &[usize]) -> ResultColumn {
        match data {
            ColumnData::Int64(vec) => {
                ResultColumn::Int64(indices.iter().map(|&i| vec[i]).collect())
            }
            ColumnData::Varchar(vec) => {
                ResultColumn::Varchar(indices.iter().map(|&i| vec[i].clone()).collect())
            }
            ColumnData::Bool(vec) => ResultColumn::Bool(indices.iter().map(|&i| vec[i]).collect()),
        }
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
                            ResultColumn::Bool(vec) => ResultColumn::Bool(
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
    use crate::api::models::BinaryOperator;
    use crate::api::models::ColumnReferenceExpression;
    use crate::api::models::ColumnarBinaryOperation;
    use crate::api::models::Literal;
    use crate::api::models::LiteralValue;
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

    /// Create a SelectQuery that selects all columns from a table
    fn select_all_columns(metastore: &Metastore, table_name: &str) -> SelectQuery {
        let table_meta = metastore
            .get_table_by_name(table_name)
            .expect("Table not found in metastore");

        SelectQuery {
            column_clauses: table_meta
                .columns
                .iter()
                .map(|col| {
                    ColumnExpression::ColumnReference(ColumnReferenceExpression {
                        table_name: Some(table_name.to_string()),
                        column_name: col.name.clone(),
                    })
                })
                .collect(),
            where_clause: None,
            order_by_clause: None,
            limit_clause: None,
        }
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

        let executor = QueryExecutor::new(metastore.clone());

        let query_def = QueryDefinition::Select(select_all_columns(&metastore, "users"));

        let query_id = executor.submit_query(query_def).unwrap();
        executor.wait_for_completion(&query_id).await.unwrap();
        let result = executor.get_result(&query_id, None).unwrap();

        assert!(result.is_some());
        let result = result.unwrap();
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_select_nonexistent_table() {
        let metastore = create_test_metastore();
        let executor = QueryExecutor::new(metastore);

        let query_def = QueryDefinition::Select(SelectQuery {
            column_clauses: vec![ColumnExpression::ColumnReference(
                ColumnReferenceExpression {
                    table_name: Some("nonexistent".to_string()),
                    column_name: "id".to_string(),
                },
            )],
            where_clause: None,
            order_by_clause: None,
            limit_clause: None,
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

        let executor = QueryExecutor::new(metastore.clone());

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
        let select_def = QueryDefinition::Select(select_all_columns(&metastore, "users"));

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

        let executor = QueryExecutor::new(metastore.clone());

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
        let select_def = QueryDefinition::Select(select_all_columns(&metastore, "employees"));
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

        let executor = QueryExecutor::new(metastore.clone());

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
        let select_def = QueryDefinition::Select(select_all_columns(&metastore, "numbers"));
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

        let executor = QueryExecutor::new(metastore.clone());

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
        let select_def = QueryDefinition::Select(select_all_columns(&metastore, "data"));
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

        let executor = QueryExecutor::new(metastore.clone());

        // Submit multiple queries
        let select1 = QueryDefinition::Select(select_all_columns(&metastore, "test"));
        let select2 = QueryDefinition::Select(select_all_columns(&metastore, "test"));

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

        let executor = QueryExecutor::new(metastore.clone());

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
        let executor = QueryExecutor::new(metastore.clone());

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

        let executor = QueryExecutor::new(metastore.clone());

        let copy_def = QueryDefinition::Copy(CopyQuery {
            source_filepath: csv_path.to_str().unwrap().to_string(),
            destination_table_name: "strings".to_string(),
            destination_columns: None,
            does_csv_contain_header: false,
        });
        let copy_id = executor.submit_query(copy_def).unwrap();
        executor.wait_for_completion(&copy_id).await.unwrap();

        let select_def = QueryDefinition::Select(select_all_columns(&metastore, "strings"));
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

        let executor = QueryExecutor::new(metastore.clone());

        let select_def = QueryDefinition::Select(select_all_columns(&metastore, "test"));
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

        let executor = QueryExecutor::new(metastore.clone());

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

        let executor = QueryExecutor::new(metastore.clone());

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

        let executor = QueryExecutor::new(metastore.clone());

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

        let executor = QueryExecutor::new(metastore.clone());

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
        let select_def = QueryDefinition::Select(select_all_columns(&metastore, "test"));
        let select_id = executor.submit_query(select_def).unwrap();
        executor.wait_for_completion(&select_id).await.unwrap();
        let result = executor.get_result(&select_id, None).unwrap().unwrap();

        assert_eq!(result[0].row_count, 2);
    }

    // ========================================================================
    // WHERE Clause Tests
    // ========================================================================

    #[tokio::test]
    async fn test_select_with_where_equals() {
        let dir = tempdir().unwrap();
        let metastore = create_persistent_metastore(dir.path());

        let columns = vec![
            ColumnMetadata {
                name: "id".to_string(),
                column_type: ColumnType::Int64,
            },
            ColumnMetadata {
                name: "status".to_string(),
                column_type: ColumnType::Varchar,
            },
        ];
        metastore
            .create_table("orders".to_string(), columns)
            .unwrap();

        // Create CSV with mixed status values
        let csv_path = dir.path().join("orders.csv");
        let mut file = std::fs::File::create(&csv_path).unwrap();
        writeln!(file, "1,completed").unwrap();
        writeln!(file, "2,pending").unwrap();
        writeln!(file, "3,completed").unwrap();
        writeln!(file, "4,cancelled").unwrap();
        writeln!(file, "5,completed").unwrap();

        let executor = QueryExecutor::new(metastore.clone());

        // COPY data
        let copy_def = QueryDefinition::Copy(CopyQuery {
            source_filepath: csv_path.to_str().unwrap().to_string(),
            destination_table_name: "orders".to_string(),
            destination_columns: None,
            does_csv_contain_header: false,
        });
        let copy_id = executor.submit_query(copy_def).unwrap();
        executor.wait_for_completion(&copy_id).await.unwrap();

        // SELECT with WHERE clause filtering for "completed" status
        let select_def = QueryDefinition::Select(SelectQuery {
            column_clauses: vec![
                ColumnExpression::ColumnReference(ColumnReferenceExpression {
                    table_name: Some("orders".to_string()),
                    column_name: "id".to_string(),
                }),
                ColumnExpression::ColumnReference(ColumnReferenceExpression {
                    table_name: Some("orders".to_string()),
                    column_name: "status".to_string(),
                }),
            ],
            where_clause: Some(ColumnExpression::BinaryOperation(ColumnarBinaryOperation {
                left_operand: Box::new(ColumnExpression::ColumnReference(
                    ColumnReferenceExpression {
                        table_name: Some("orders".to_string()),
                        column_name: "status".to_string(),
                    },
                )),
                operator: BinaryOperator::Equal,
                right_operand: Box::new(ColumnExpression::Literal(Literal {
                    value: LiteralValue::Varchar("completed".to_string()),
                })),
            })),
            order_by_clause: None,
            limit_clause: None,
        });

        let select_id = executor.submit_query(select_def).unwrap();
        executor.wait_for_completion(&select_id).await.unwrap();
        let result = executor.get_result(&select_id, None).unwrap().unwrap();

        // Should return 3 rows (id 1, 3, 5 with status "completed")
        assert_eq!(result[0].row_count, 3);
    }

    #[tokio::test]
    async fn test_select_with_where_greater_than() {
        let dir = tempdir().unwrap();
        let metastore = create_persistent_metastore(dir.path());

        let columns = vec![
            ColumnMetadata {
                name: "id".to_string(),
                column_type: ColumnType::Int64,
            },
            ColumnMetadata {
                name: "salary".to_string(),
                column_type: ColumnType::Int64,
            },
        ];
        metastore
            .create_table("employees".to_string(), columns)
            .unwrap();

        // Create CSV with salary data
        let csv_path = dir.path().join("employees.csv");
        let mut file = std::fs::File::create(&csv_path).unwrap();
        writeln!(file, "1,50000").unwrap();
        writeln!(file, "2,75000").unwrap();
        writeln!(file, "3,60000").unwrap();
        writeln!(file, "4,100000").unwrap();
        writeln!(file, "5,55000").unwrap();

        let executor = QueryExecutor::new(metastore.clone());

        let copy_def = QueryDefinition::Copy(CopyQuery {
            source_filepath: csv_path.to_str().unwrap().to_string(),
            destination_table_name: "employees".to_string(),
            destination_columns: None,
            does_csv_contain_header: false,
        });
        let copy_id = executor.submit_query(copy_def).unwrap();
        executor.wait_for_completion(&copy_id).await.unwrap();

        // SELECT with WHERE salary > 60000
        let select_def = QueryDefinition::Select(SelectQuery {
            column_clauses: vec![
                ColumnExpression::ColumnReference(ColumnReferenceExpression {
                    table_name: Some("employees".to_string()),
                    column_name: "id".to_string(),
                }),
                ColumnExpression::ColumnReference(ColumnReferenceExpression {
                    table_name: Some("employees".to_string()),
                    column_name: "salary".to_string(),
                }),
            ],
            where_clause: Some(ColumnExpression::BinaryOperation(ColumnarBinaryOperation {
                left_operand: Box::new(ColumnExpression::ColumnReference(
                    ColumnReferenceExpression {
                        table_name: Some("employees".to_string()),
                        column_name: "salary".to_string(),
                    },
                )),
                operator: BinaryOperator::GreaterThan,
                right_operand: Box::new(ColumnExpression::Literal(Literal {
                    value: LiteralValue::Int64(60000),
                })),
            })),
            order_by_clause: None,
            limit_clause: None,
        });

        let select_id = executor.submit_query(select_def).unwrap();
        executor.wait_for_completion(&select_id).await.unwrap();
        let result = executor.get_result(&select_id, None).unwrap().unwrap();

        // Should return 2 rows (id 2,4 with salary > 60000)
        assert_eq!(result[0].row_count, 2);
    }

    #[tokio::test]
    async fn test_select_with_where_no_matches() {
        let dir = tempdir().unwrap();
        let metastore = create_persistent_metastore(dir.path());

        let columns = vec![ColumnMetadata {
            name: "value".to_string(),
            column_type: ColumnType::Int64,
        }];
        metastore
            .create_table("numbers".to_string(), columns)
            .unwrap();

        let csv_path = dir.path().join("numbers.csv");
        let mut file = std::fs::File::create(&csv_path).unwrap();
        writeln!(file, "1").unwrap();
        writeln!(file, "2").unwrap();
        writeln!(file, "3").unwrap();

        let executor = QueryExecutor::new(metastore.clone());

        let copy_def = QueryDefinition::Copy(CopyQuery {
            source_filepath: csv_path.to_str().unwrap().to_string(),
            destination_table_name: "numbers".to_string(),
            destination_columns: None,
            does_csv_contain_header: false,
        });
        let copy_id = executor.submit_query(copy_def).unwrap();
        executor.wait_for_completion(&copy_id).await.unwrap();

        // SELECT with WHERE that matches nothing
        let select_def = QueryDefinition::Select(SelectQuery {
            column_clauses: vec![ColumnExpression::ColumnReference(
                ColumnReferenceExpression {
                    table_name: Some("numbers".to_string()),
                    column_name: "value".to_string(),
                },
            )],
            where_clause: Some(ColumnExpression::BinaryOperation(ColumnarBinaryOperation {
                left_operand: Box::new(ColumnExpression::ColumnReference(
                    ColumnReferenceExpression {
                        table_name: Some("numbers".to_string()),
                        column_name: "value".to_string(),
                    },
                )),
                operator: BinaryOperator::GreaterThan,
                right_operand: Box::new(ColumnExpression::Literal(Literal {
                    value: LiteralValue::Int64(100),
                })),
            })),
            order_by_clause: None,
            limit_clause: None,
        });

        let select_id = executor.submit_query(select_def).unwrap();
        executor.wait_for_completion(&select_id).await.unwrap();
        let result = executor.get_result(&select_id, None).unwrap().unwrap();

        // Should return empty result array (no rows match WHERE)
        assert_eq!(result.len(), 0);
    }

    // ========================================================================
    // ORDER BY Clause Tests
    // ========================================================================

    #[tokio::test]
    async fn test_select_with_order_by_ascending() {
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
                name: "score".to_string(),
                column_type: ColumnType::Int64,
            },
        ];
        metastore
            .create_table("scores".to_string(), columns)
            .unwrap();

        // Create CSV with unsorted data
        let csv_path = dir.path().join("scores.csv");
        let mut file = std::fs::File::create(&csv_path).unwrap();
        writeln!(file, "1,Alice,95").unwrap();
        writeln!(file, "2,Bob,78").unwrap();
        writeln!(file, "3,Charlie,92").unwrap();
        writeln!(file, "4,David,85").unwrap();

        let executor = QueryExecutor::new(metastore.clone());

        let copy_def = QueryDefinition::Copy(CopyQuery {
            source_filepath: csv_path.to_str().unwrap().to_string(),
            destination_table_name: "scores".to_string(),
            destination_columns: None,
            does_csv_contain_header: false,
        });
        let copy_id = executor.submit_query(copy_def).unwrap();
        executor.wait_for_completion(&copy_id).await.unwrap();

        // SELECT with ORDER BY score ascending
        let select_def = QueryDefinition::Select(SelectQuery {
            column_clauses: vec![
                ColumnExpression::ColumnReference(ColumnReferenceExpression {
                    table_name: Some("scores".to_string()),
                    column_name: "id".to_string(),
                }),
                ColumnExpression::ColumnReference(ColumnReferenceExpression {
                    table_name: Some("scores".to_string()),
                    column_name: "score".to_string(),
                }),
            ],
            where_clause: None,
            order_by_clause: Some(vec![OrderByExpression {
                column_index: 1, // score column
                ascending: true,
            }]),
            limit_clause: None,
        });

        let select_id = executor.submit_query(select_def).unwrap();
        executor.wait_for_completion(&select_id).await.unwrap();
        let result = executor.get_result(&select_id, None).unwrap().unwrap();

        assert_eq!(result[0].row_count, 4);
        // Verify scores are in ascending order: 78, 85, 92, 95
        if let ResultColumn::Int64(scores) = &result[0].columns[1] {
            assert_eq!(scores, &vec![78, 85, 92, 95]);
        } else {
            panic!("Expected INT64 column");
        }
    }

    #[tokio::test]
    async fn test_select_with_order_by_descending() {
        let dir = tempdir().unwrap();
        let metastore = create_persistent_metastore(dir.path());

        let columns = vec![
            ColumnMetadata {
                name: "product".to_string(),
                column_type: ColumnType::Varchar,
            },
            ColumnMetadata {
                name: "price".to_string(),
                column_type: ColumnType::Int64,
            },
        ];
        metastore
            .create_table("products".to_string(), columns)
            .unwrap();

        let csv_path = dir.path().join("products.csv");
        let mut file = std::fs::File::create(&csv_path).unwrap();
        writeln!(file, "Apple,10").unwrap();
        writeln!(file, "Banana,5").unwrap();
        writeln!(file, "Orange,8").unwrap();
        writeln!(file, "Mango,15").unwrap();

        let executor = QueryExecutor::new(metastore.clone());

        let copy_def = QueryDefinition::Copy(CopyQuery {
            source_filepath: csv_path.to_str().unwrap().to_string(),
            destination_table_name: "products".to_string(),
            destination_columns: None,
            does_csv_contain_header: false,
        });
        let copy_id = executor.submit_query(copy_def).unwrap();
        executor.wait_for_completion(&copy_id).await.unwrap();

        // SELECT with ORDER BY price descending
        let select_def = QueryDefinition::Select(SelectQuery {
            column_clauses: vec![
                ColumnExpression::ColumnReference(ColumnReferenceExpression {
                    table_name: Some("products".to_string()),
                    column_name: "product".to_string(),
                }),
                ColumnExpression::ColumnReference(ColumnReferenceExpression {
                    table_name: Some("products".to_string()),
                    column_name: "price".to_string(),
                }),
            ],
            where_clause: None,
            order_by_clause: Some(vec![OrderByExpression {
                column_index: 1, // price column
                ascending: false,
            }]),
            limit_clause: None,
        });

        let select_id = executor.submit_query(select_def).unwrap();
        executor.wait_for_completion(&select_id).await.unwrap();
        let result = executor.get_result(&select_id, None).unwrap().unwrap();

        assert_eq!(result[0].row_count, 4);
        // Verify prices are in descending order: 15, 10, 8, 5
        if let ResultColumn::Int64(prices) = &result[0].columns[1] {
            assert_eq!(prices, &vec![15, 10, 8, 5]);
        } else {
            panic!("Expected INT64 column");
        }
    }

    #[tokio::test]
    async fn test_select_with_order_by_multiple_columns() {
        let dir = tempdir().unwrap();
        let metastore = create_persistent_metastore(dir.path());

        let columns = vec![
            ColumnMetadata {
                name: "department".to_string(),
                column_type: ColumnType::Varchar,
            },
            ColumnMetadata {
                name: "salary".to_string(),
                column_type: ColumnType::Int64,
            },
            ColumnMetadata {
                name: "name".to_string(),
                column_type: ColumnType::Varchar,
            },
        ];
        metastore
            .create_table("staff".to_string(), columns)
            .unwrap();

        let csv_path = dir.path().join("staff.csv");
        let mut file = std::fs::File::create(&csv_path).unwrap();
        writeln!(file, "Sales,50000,Alice").unwrap();
        writeln!(file, "HR,40000,Bob").unwrap();
        writeln!(file, "Sales,55000,Charlie").unwrap();
        writeln!(file, "HR,45000,Diana").unwrap();

        let executor = QueryExecutor::new(metastore.clone());

        let copy_def = QueryDefinition::Copy(CopyQuery {
            source_filepath: csv_path.to_str().unwrap().to_string(),
            destination_table_name: "staff".to_string(),
            destination_columns: None,
            does_csv_contain_header: false,
        });
        let copy_id = executor.submit_query(copy_def).unwrap();
        executor.wait_for_completion(&copy_id).await.unwrap();

        // SELECT with ORDER BY department ascending, then salary descending
        let select_def = QueryDefinition::Select(SelectQuery {
            column_clauses: vec![
                ColumnExpression::ColumnReference(ColumnReferenceExpression {
                    table_name: Some("staff".to_string()),
                    column_name: "department".to_string(),
                }),
                ColumnExpression::ColumnReference(ColumnReferenceExpression {
                    table_name: Some("staff".to_string()),
                    column_name: "salary".to_string(),
                }),
            ],
            where_clause: None,
            order_by_clause: Some(vec![
                OrderByExpression {
                    column_index: 0, // department
                    ascending: true,
                },
                OrderByExpression {
                    column_index: 1, // salary
                    ascending: false,
                },
            ]),
            limit_clause: None,
        });

        let select_id = executor.submit_query(select_def).unwrap();
        executor.wait_for_completion(&select_id).await.unwrap();
        let result = executor.get_result(&select_id, None).unwrap().unwrap();

        assert_eq!(result[0].row_count, 4);
        // Verify: HR (40k, 45k), Sales (55k, 50k)
        if let ResultColumn::Varchar(depts) = &result[0].columns[0] {
            assert_eq!(depts, &vec!["HR", "HR", "Sales", "Sales"]);
        } else {
            panic!("Expected VARCHAR column");
        }
        if let ResultColumn::Int64(salaries) = &result[0].columns[1] {
            assert_eq!(salaries, &vec![45000, 40000, 55000, 50000]);
        } else {
            panic!("Expected INT64 column");
        }
    }

    #[tokio::test]
    async fn test_select_with_order_by_string_column() {
        let dir = tempdir().unwrap();
        let metastore = create_persistent_metastore(dir.path());

        let columns = vec![ColumnMetadata {
            name: "city".to_string(),
            column_type: ColumnType::Varchar,
        }];
        metastore
            .create_table("cities".to_string(), columns)
            .unwrap();

        let csv_path = dir.path().join("cities.csv");
        let mut file = std::fs::File::create(&csv_path).unwrap();
        writeln!(file, "Zebra").unwrap();
        writeln!(file, "Apple").unwrap();
        writeln!(file, "Monkey").unwrap();
        writeln!(file, "Banana").unwrap();

        let executor = QueryExecutor::new(metastore.clone());

        let copy_def = QueryDefinition::Copy(CopyQuery {
            source_filepath: csv_path.to_str().unwrap().to_string(),
            destination_table_name: "cities".to_string(),
            destination_columns: None,
            does_csv_contain_header: false,
        });
        let copy_id = executor.submit_query(copy_def).unwrap();
        executor.wait_for_completion(&copy_id).await.unwrap();

        // SELECT with ORDER BY city ascending
        let select_def = QueryDefinition::Select(SelectQuery {
            column_clauses: vec![ColumnExpression::ColumnReference(
                ColumnReferenceExpression {
                    table_name: Some("cities".to_string()),
                    column_name: "city".to_string(),
                },
            )],
            where_clause: None,
            order_by_clause: Some(vec![OrderByExpression {
                column_index: 0,
                ascending: true,
            }]),
            limit_clause: None,
        });

        let select_id = executor.submit_query(select_def).unwrap();
        executor.wait_for_completion(&select_id).await.unwrap();
        let result = executor.get_result(&select_id, None).unwrap().unwrap();

        assert_eq!(result[0].row_count, 4);
        if let ResultColumn::Varchar(cities) = &result[0].columns[0] {
            assert_eq!(cities, &vec!["Apple", "Banana", "Monkey", "Zebra"]);
        } else {
            panic!("Expected VARCHAR column");
        }
    }

    // ========================================================================
    // LIMIT Clause Tests
    // ========================================================================

    #[tokio::test]
    async fn test_select_with_limit() {
        let dir = tempdir().unwrap();
        let metastore = create_persistent_metastore(dir.path());

        let columns = vec![ColumnMetadata {
            name: "value".to_string(),
            column_type: ColumnType::Int64,
        }];
        metastore
            .create_table("numbers".to_string(), columns)
            .unwrap();

        // Create CSV with 10 rows
        let csv_path = dir.path().join("numbers.csv");
        let mut file = std::fs::File::create(&csv_path).unwrap();
        for i in 1..=10 {
            writeln!(file, "{}", i).unwrap();
        }

        let executor = QueryExecutor::new(metastore.clone());

        let copy_def = QueryDefinition::Copy(CopyQuery {
            source_filepath: csv_path.to_str().unwrap().to_string(),
            destination_table_name: "numbers".to_string(),
            destination_columns: None,
            does_csv_contain_header: false,
        });
        let copy_id = executor.submit_query(copy_def).unwrap();
        executor.wait_for_completion(&copy_id).await.unwrap();

        // SELECT with LIMIT 5
        let select_def = QueryDefinition::Select(SelectQuery {
            column_clauses: vec![ColumnExpression::ColumnReference(
                ColumnReferenceExpression {
                    table_name: Some("numbers".to_string()),
                    column_name: "value".to_string(),
                },
            )],
            where_clause: None,
            order_by_clause: None,
            limit_clause: Some(LimitExpression { limit: 5 }),
        });

        let select_id = executor.submit_query(select_def).unwrap();
        executor.wait_for_completion(&select_id).await.unwrap();
        let result = executor.get_result(&select_id, None).unwrap().unwrap();

        // Should return only 5 rows
        assert_eq!(result[0].row_count, 5);
        if let ResultColumn::Int64(values) = &result[0].columns[0] {
            assert_eq!(values, &vec![1, 2, 3, 4, 5]);
        } else {
            panic!("Expected INT64 column");
        }
    }

    #[tokio::test]
    async fn test_select_with_limit_exceeds_rows() {
        let dir = tempdir().unwrap();
        let metastore = create_persistent_metastore(dir.path());

        let columns = vec![ColumnMetadata {
            name: "value".to_string(),
            column_type: ColumnType::Int64,
        }];
        metastore.create_table("data".to_string(), columns).unwrap();

        let csv_path = dir.path().join("data.csv");
        let mut file = std::fs::File::create(&csv_path).unwrap();
        writeln!(file, "1").unwrap();
        writeln!(file, "2").unwrap();
        writeln!(file, "3").unwrap();

        let executor = QueryExecutor::new(metastore.clone());

        let copy_def = QueryDefinition::Copy(CopyQuery {
            source_filepath: csv_path.to_str().unwrap().to_string(),
            destination_table_name: "data".to_string(),
            destination_columns: None,
            does_csv_contain_header: false,
        });
        let copy_id = executor.submit_query(copy_def).unwrap();
        executor.wait_for_completion(&copy_id).await.unwrap();

        // SELECT with LIMIT 100 (more than available rows)
        let select_def = QueryDefinition::Select(SelectQuery {
            column_clauses: vec![ColumnExpression::ColumnReference(
                ColumnReferenceExpression {
                    table_name: Some("data".to_string()),
                    column_name: "value".to_string(),
                },
            )],
            where_clause: None,
            order_by_clause: None,
            limit_clause: Some(LimitExpression { limit: 100 }),
        });

        let select_id = executor.submit_query(select_def).unwrap();
        executor.wait_for_completion(&select_id).await.unwrap();
        let result = executor.get_result(&select_id, None).unwrap().unwrap();

        // Should return only 3 rows
        assert_eq!(result[0].row_count, 3);
    }

    #[tokio::test]
    async fn test_select_with_limit_zero() {
        let dir = tempdir().unwrap();
        let metastore = create_persistent_metastore(dir.path());

        let columns = vec![ColumnMetadata {
            name: "value".to_string(),
            column_type: ColumnType::Int64,
        }];
        metastore.create_table("test".to_string(), columns).unwrap();

        let csv_path = dir.path().join("test.csv");
        let mut file = std::fs::File::create(&csv_path).unwrap();
        writeln!(file, "1").unwrap();
        writeln!(file, "2").unwrap();

        let executor = QueryExecutor::new(metastore.clone());

        let copy_def = QueryDefinition::Copy(CopyQuery {
            source_filepath: csv_path.to_str().unwrap().to_string(),
            destination_table_name: "test".to_string(),
            destination_columns: None,
            does_csv_contain_header: false,
        });
        let copy_id = executor.submit_query(copy_def).unwrap();
        executor.wait_for_completion(&copy_id).await.unwrap();

        // SELECT with LIMIT 0
        let select_def = QueryDefinition::Select(SelectQuery {
            column_clauses: vec![ColumnExpression::ColumnReference(
                ColumnReferenceExpression {
                    table_name: Some("test".to_string()),
                    column_name: "value".to_string(),
                },
            )],
            where_clause: None,
            order_by_clause: None,
            limit_clause: Some(LimitExpression { limit: 0 }),
        });

        let select_id = executor.submit_query(select_def).unwrap();
        executor.wait_for_completion(&select_id).await.unwrap();
        let result = executor.get_result(&select_id, None).unwrap().unwrap();

        // Should return empty result array (LIMIT 0)
        assert_eq!(result.len(), 0);
    }

    // ========================================================================
    // Combined WHERE + ORDER BY + LIMIT Tests
    // ========================================================================

    #[tokio::test]
    async fn test_select_with_where_order_by_limit() {
        let dir = tempdir().unwrap();
        let metastore = create_persistent_metastore(dir.path());

        let columns = vec![
            ColumnMetadata {
                name: "id".to_string(),
                column_type: ColumnType::Int64,
            },
            ColumnMetadata {
                name: "category".to_string(),
                column_type: ColumnType::Varchar,
            },
            ColumnMetadata {
                name: "price".to_string(),
                column_type: ColumnType::Int64,
            },
        ];
        metastore
            .create_table("items".to_string(), columns)
            .unwrap();

        let csv_path = dir.path().join("items.csv");
        let mut file = std::fs::File::create(&csv_path).unwrap();
        writeln!(file, "1,book,20").unwrap();
        writeln!(file, "2,pen,5").unwrap();
        writeln!(file, "3,book,15").unwrap();
        writeln!(file, "4,pen,8").unwrap();
        writeln!(file, "5,book,25").unwrap();
        writeln!(file, "6,pen,10").unwrap();
        writeln!(file, "7,book,12").unwrap();

        let executor = QueryExecutor::new(metastore.clone());

        let copy_def = QueryDefinition::Copy(CopyQuery {
            source_filepath: csv_path.to_str().unwrap().to_string(),
            destination_table_name: "items".to_string(),
            destination_columns: None,
            does_csv_contain_header: false,
        });
        let copy_id = executor.submit_query(copy_def).unwrap();
        executor.wait_for_completion(&copy_id).await.unwrap();

        // SELECT books with price DESC, LIMIT 2
        let select_def = QueryDefinition::Select(SelectQuery {
            column_clauses: vec![
                ColumnExpression::ColumnReference(ColumnReferenceExpression {
                    table_name: Some("items".to_string()),
                    column_name: "id".to_string(),
                }),
                ColumnExpression::ColumnReference(ColumnReferenceExpression {
                    table_name: Some("items".to_string()),
                    column_name: "price".to_string(),
                }),
            ],
            where_clause: Some(ColumnExpression::BinaryOperation(ColumnarBinaryOperation {
                left_operand: Box::new(ColumnExpression::ColumnReference(
                    ColumnReferenceExpression {
                        table_name: Some("items".to_string()),
                        column_name: "category".to_string(),
                    },
                )),
                operator: BinaryOperator::Equal,
                right_operand: Box::new(ColumnExpression::Literal(Literal {
                    value: LiteralValue::Varchar("book".to_string()),
                })),
            })),
            order_by_clause: Some(vec![OrderByExpression {
                column_index: 1, // price
                ascending: false,
            }]),
            limit_clause: Some(LimitExpression { limit: 2 }),
        });

        let select_id = executor.submit_query(select_def).unwrap();
        executor.wait_for_completion(&select_id).await.unwrap();
        let result = executor.get_result(&select_id, None).unwrap().unwrap();

        // Should return 2 rows: id 5 (price 25), id 1 (price 20)
        assert_eq!(result[0].row_count, 2);
        if let ResultColumn::Int64(ids) = &result[0].columns[0] {
            assert_eq!(ids, &vec![5, 1]);
        } else {
            panic!("Expected INT64 column for ids");
        }
        if let ResultColumn::Int64(prices) = &result[0].columns[1] {
            assert_eq!(prices, &vec![25, 20]);
        } else {
            panic!("Expected INT64 column for prices");
        }
    }

    #[tokio::test]
    async fn test_select_with_where_and_limit_no_order() {
        let dir = tempdir().unwrap();
        let metastore = create_persistent_metastore(dir.path());

        let columns = vec![
            ColumnMetadata {
                name: "status".to_string(),
                column_type: ColumnType::Varchar,
            },
            ColumnMetadata {
                name: "value".to_string(),
                column_type: ColumnType::Int64,
            },
        ];
        metastore
            .create_table("records".to_string(), columns)
            .unwrap();

        let csv_path = dir.path().join("records.csv");
        let mut file = std::fs::File::create(&csv_path).unwrap();
        writeln!(file, "active,100").unwrap();
        writeln!(file, "inactive,200").unwrap();
        writeln!(file, "active,150").unwrap();
        writeln!(file, "inactive,250").unwrap();
        writeln!(file, "active,200").unwrap();

        let executor = QueryExecutor::new(metastore.clone());

        let copy_def = QueryDefinition::Copy(CopyQuery {
            source_filepath: csv_path.to_str().unwrap().to_string(),
            destination_table_name: "records".to_string(),
            destination_columns: None,
            does_csv_contain_header: false,
        });
        let copy_id = executor.submit_query(copy_def).unwrap();
        executor.wait_for_completion(&copy_id).await.unwrap();

        // SELECT active records with LIMIT 2 (no ORDER BY)
        let select_def = QueryDefinition::Select(SelectQuery {
            column_clauses: vec![
                ColumnExpression::ColumnReference(ColumnReferenceExpression {
                    table_name: Some("records".to_string()),
                    column_name: "status".to_string(),
                }),
                ColumnExpression::ColumnReference(ColumnReferenceExpression {
                    table_name: Some("records".to_string()),
                    column_name: "value".to_string(),
                }),
            ],
            where_clause: Some(ColumnExpression::BinaryOperation(ColumnarBinaryOperation {
                left_operand: Box::new(ColumnExpression::ColumnReference(
                    ColumnReferenceExpression {
                        table_name: Some("records".to_string()),
                        column_name: "status".to_string(),
                    },
                )),
                operator: BinaryOperator::Equal,
                right_operand: Box::new(ColumnExpression::Literal(Literal {
                    value: LiteralValue::Varchar("active".to_string()),
                })),
            })),
            order_by_clause: None,
            limit_clause: Some(LimitExpression { limit: 2 }),
        });

        let select_id = executor.submit_query(select_def).unwrap();
        executor.wait_for_completion(&select_id).await.unwrap();
        let result = executor.get_result(&select_id, None).unwrap().unwrap();

        // Should return 2 rows (first 2 "active" records)
        assert_eq!(result[0].row_count, 2);
    }
}
