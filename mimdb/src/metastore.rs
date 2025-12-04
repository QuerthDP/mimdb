/*
 * Copyright (c) 2025-present Dawid Pawlik
 *
 * For educational use only by employees and students of MIMUW.
 * See LICENSE file for details.
 */

//! # Metastore - Logical Database Structure
//!
//! This module provides the metastore functionality, which translates logical database
//! abstractions (tables, columns) to the physical storage layer.
//!
//! The metastore is persisted to disk and survives database restarts.

use crate::ColumnType;
use anyhow::Context;
use anyhow::Result;
use parking_lot::Mutex;
use parking_lot::RwLock;
use serde::Deserialize;
use serde::Serialize;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use uuid::Uuid;

/// Metadata for a single column in a table
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnMetadata {
    pub name: String,
    pub column_type: ColumnType,
}

/// Metadata for a table in the database
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableMetadata {
    pub table_id: String,
    pub name: String,
    pub columns: Vec<ColumnMetadata>,
    /// List of data files associated with this table
    pub data_files: Vec<PathBuf>,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

impl TableMetadata {
    /// Create new table metadata
    pub fn new(name: String, columns: Vec<ColumnMetadata>) -> Self {
        Self {
            table_id: Uuid::new_v4().to_string(),
            name,
            columns,
            data_files: Vec::new(),
            created_at: chrono::Utc::now(),
        }
    }

    /// Add a data file to this table
    pub fn add_data_file(&mut self, path: PathBuf) {
        self.data_files.push(path);
    }
}

/// Information about a table pending deletion
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingDeletion {
    pub table_id: String,
    pub data_files: Vec<PathBuf>,
    pub table_dir: PathBuf,
}

/// The metastore - maps logical table names to physical storage
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct MetastoreData {
    /// Tables indexed by their ID
    pub tables: HashMap<String, TableMetadata>,
    /// Index from table name to table ID for quick lookups
    #[serde(default)]
    pub name_to_id: HashMap<String, String>,
    /// Tables that have been logically deleted but have files pending physical removal
    #[serde(default)]
    pub pending_deletions: Vec<PendingDeletion>,
}

/// Tracks which queries are currently accessing which tables
#[derive(Debug, Default)]
pub struct TableAccessTracker {
    /// Maps table_id -> set of query_ids currently accessing the table
    active_accesses: HashMap<String, HashSet<String>>,
}

impl TableAccessTracker {
    pub fn new() -> Self {
        Self {
            active_accesses: HashMap::new(),
        }
    }

    /// Register that a query is accessing a table
    pub fn acquire(&mut self, table_id: &str, query_id: &str) {
        self.active_accesses
            .entry(table_id.to_string())
            .or_default()
            .insert(query_id.to_string());
    }

    /// Release a query's access to a table
    pub fn release(&mut self, table_id: &str, query_id: &str) {
        if let Some(queries) = self.active_accesses.get_mut(table_id) {
            queries.remove(query_id);
            if queries.is_empty() {
                self.active_accesses.remove(table_id);
            }
        }
    }

    /// Check if a table has any active accesses
    pub fn has_active_accesses(&self, table_id: &str) -> bool {
        self.active_accesses
            .get(table_id)
            .map(|s| !s.is_empty())
            .unwrap_or(false)
    }

    /// Get the number of active accesses for a table
    pub fn access_count(&self, table_id: &str) -> usize {
        self.active_accesses
            .get(table_id)
            .map(|s| s.len())
            .unwrap_or(0)
    }
}

/// Thread-safe metastore with persistence
#[derive(Debug)]
pub struct Metastore {
    data: Arc<RwLock<MetastoreData>>,
    storage_path: PathBuf,
    data_directory: PathBuf,
    /// Tracks active query accesses to tables
    access_tracker: Arc<Mutex<TableAccessTracker>>,
}

impl Metastore {
    const METASTORE_FILENAME: &'static str = "metastore.json";

    /// Create or load metastore from the given directory
    pub fn new<P: AsRef<Path>>(storage_directory: P) -> Result<Self> {
        let storage_path = storage_directory.as_ref().to_path_buf();
        let data_directory = storage_path.join("tables");

        // Ensure directories exist
        fs::create_dir_all(&storage_path).context("Failed to create storage directory")?;
        fs::create_dir_all(&data_directory).context("Failed to create data directory")?;

        let metastore_file = storage_path.join(Self::METASTORE_FILENAME);

        let data = if metastore_file.exists() {
            let content =
                fs::read_to_string(&metastore_file).context("Failed to read metastore file")?;
            serde_json::from_str(&content).context("Failed to parse metastore file")?
        } else {
            MetastoreData::default()
        };

        let metastore = Self {
            data: Arc::new(RwLock::new(data)),
            storage_path,
            data_directory,
            access_tracker: Arc::new(Mutex::new(TableAccessTracker::new())),
        };

        // Clean up any pending deletions from previous runs (no active queries on startup)
        metastore.cleanup_pending_deletions()?;

        Ok(metastore)
    }

    /// Persist metastore to disk
    pub fn persist(&self) -> Result<()> {
        let data = self.data.read();
        let content =
            serde_json::to_string_pretty(&*data).context("Failed to serialize metastore")?;

        let metastore_file = self.storage_path.join(Self::METASTORE_FILENAME);
        fs::write(&metastore_file, content).context("Failed to write metastore file")?;

        Ok(())
    }

    /// List all tables (shallow representation)
    pub fn list_tables(&self) -> Vec<(String, String)> {
        let data = self.data.read();
        data.tables
            .values()
            .map(|t| (t.table_id.clone(), t.name.clone()))
            .collect()
    }

    /// Get table by ID
    pub fn get_table(&self, table_id: &str) -> Option<TableMetadata> {
        let data = self.data.read();
        data.tables.get(table_id).cloned()
    }

    /// Get table by name
    pub fn get_table_by_name(&self, name: &str) -> Option<TableMetadata> {
        let data = self.data.read();
        data.name_to_id
            .get(name)
            .and_then(|id| data.tables.get(id))
            .cloned()
    }

    /// Check if a table with the given name exists
    pub fn table_exists(&self, name: &str) -> bool {
        let data = self.data.read();
        data.name_to_id.contains_key(name)
    }

    /// Create a new table
    pub fn create_table(
        &self,
        name: String,
        columns: Vec<ColumnMetadata>,
    ) -> Result<TableMetadata> {
        let mut data = self.data.write();

        // Check if table with this name already exists
        if data.name_to_id.contains_key(&name) {
            anyhow::bail!("Table '{}' already exists", name);
        }

        // Validate column names are unique
        let mut seen_names = std::collections::HashSet::new();
        for col in &columns {
            if !seen_names.insert(&col.name) {
                anyhow::bail!("Duplicate column name: '{}'", col.name);
            }
        }

        let table = TableMetadata::new(name.clone(), columns);
        let table_id = table.table_id.clone();

        // Create table directory
        let table_dir = self.data_directory.join(&table_id);
        fs::create_dir_all(&table_dir).context("Failed to create table directory")?;

        data.tables.insert(table_id.clone(), table.clone());
        data.name_to_id.insert(name, table_id);

        drop(data);
        self.persist()?;

        Ok(table)
    }

    /// Delete a table by ID
    ///
    /// The table is immediately removed from the logical view (subsequent queries won't see it),
    /// but physical files are only deleted when no active queries are using them.
    pub fn delete_table(&self, table_id: &str) -> Result<TableMetadata> {
        let mut data = self.data.write();

        let table = data
            .tables
            .remove(table_id)
            .ok_or_else(|| anyhow::anyhow!("Table not found: {}", table_id))?;

        data.name_to_id.remove(&table.name);

        let table_dir = self.data_directory.join(table_id);

        // Check if there are active queries using this table
        let tracker = self.access_tracker.lock();
        let has_active_queries = tracker.has_active_accesses(table_id);
        drop(tracker);

        if has_active_queries {
            // Table has active queries - schedule files for deletion later
            data.pending_deletions.push(PendingDeletion {
                table_id: table_id.to_string(),
                data_files: table.data_files.clone(),
                table_dir,
            });
        } else {
            // No active queries - delete files immediately
            if table_dir.exists() {
                for file in &table.data_files {
                    let _ = fs::remove_file(file);
                }
                let _ = fs::remove_dir_all(&table_dir);
            }
        }

        drop(data);
        self.persist()?;

        Ok(table)
    }

    /// Add a data file to a table
    pub fn add_data_file(&self, table_id: &str, file_path: PathBuf) -> Result<()> {
        let mut data = self.data.write();

        let table = data
            .tables
            .get_mut(table_id)
            .ok_or_else(|| anyhow::anyhow!("Table not found: {}", table_id))?;

        table.data_files.push(file_path);

        drop(data);
        self.persist()?;

        Ok(())
    }

    /// Generate a new data file path for a table
    pub fn generate_data_file_path(&self, table_id: &str) -> PathBuf {
        let file_id = Uuid::new_v4();
        self.data_directory
            .join(table_id)
            .join(format!("{}.mimdb", file_id))
    }

    /// Acquire access to a table for a query.
    /// This must be called before a query starts reading from a table.
    /// The table must still exist (not be logically deleted) for this to succeed.
    pub fn acquire_table_access(&self, table_id: &str, query_id: &str) -> Result<()> {
        // First verify the table still exists
        let data = self.data.read();
        if !data.tables.contains_key(table_id) {
            anyhow::bail!("Table '{}' does not exist", table_id);
        }
        drop(data);

        // Register the access
        let mut tracker = self.access_tracker.lock();
        tracker.acquire(table_id, query_id);
        Ok(())
    }

    /// Release access to a table for a query.
    /// This must be called when a query finishes (successfully or with error).
    /// This may trigger cleanup of pending deletions if this was the last query.
    pub fn release_table_access(&self, table_id: &str, query_id: &str) {
        let mut tracker = self.access_tracker.lock();
        tracker.release(table_id, query_id);
        let has_active = tracker.has_active_accesses(table_id);
        drop(tracker);

        // If no more active accesses, try to clean up pending deletion for this table
        if !has_active {
            let _ = self.try_cleanup_table(table_id);
        }
    }

    /// Try to cleanup files for a specific table if it's pending deletion
    fn try_cleanup_table(&self, table_id: &str) -> Result<()> {
        let mut data = self.data.write();

        // Find and remove the pending deletion for this table
        let pos = data
            .pending_deletions
            .iter()
            .position(|p| p.table_id == table_id);

        if let Some(idx) = pos {
            let pending = data.pending_deletions.remove(idx);

            // Delete the files
            for file in &pending.data_files {
                let _ = fs::remove_file(file);
            }
            if pending.table_dir.exists() {
                let _ = fs::remove_dir_all(&pending.table_dir);
            }

            drop(data);
            self.persist()?;
        }

        Ok(())
    }

    /// Clean up all pending deletions that have no active queries.
    /// Called on startup and can be called periodically.
    pub fn cleanup_pending_deletions(&self) -> Result<()> {
        let tracker = self.access_tracker.lock();
        let mut data = self.data.write();

        let mut remaining = Vec::new();
        for pending in std::mem::take(&mut data.pending_deletions) {
            if tracker.has_active_accesses(&pending.table_id) {
                // Still has active queries, keep pending
                remaining.push(pending);
            } else {
                // No active queries, delete files
                for file in &pending.data_files {
                    let _ = fs::remove_file(file);
                }
                if pending.table_dir.exists() {
                    let _ = fs::remove_dir_all(&pending.table_dir);
                }
            }
        }

        data.pending_deletions = remaining;
        drop(tracker);
        drop(data);

        self.persist()?;
        Ok(())
    }

    /// Check if a table has pending deletion
    pub fn is_pending_deletion(&self, table_id: &str) -> bool {
        let data = self.data.read();
        data.pending_deletions
            .iter()
            .any(|p| p.table_id == table_id)
    }

    /// Get the number of active accesses for a table
    pub fn active_access_count(&self, table_id: &str) -> usize {
        let tracker = self.access_tracker.lock();
        tracker.access_count(table_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_metastore_creation() {
        let dir = tempdir().unwrap();
        let metastore = Metastore::new(dir.path()).unwrap();

        assert!(metastore.list_tables().is_empty());
    }

    #[test]
    fn test_create_and_list_table() {
        let dir = tempdir().unwrap();
        let metastore = Metastore::new(dir.path()).unwrap();

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

        let table = metastore
            .create_table("users".to_string(), columns)
            .unwrap();

        assert_eq!(table.name, "users");
        assert_eq!(metastore.list_tables().len(), 1);
    }

    #[test]
    fn test_duplicate_table_name() {
        let dir = tempdir().unwrap();
        let metastore = Metastore::new(dir.path()).unwrap();

        let columns = vec![ColumnMetadata {
            name: "id".to_string(),
            column_type: ColumnType::Int64,
        }];

        metastore
            .create_table("users".to_string(), columns.clone())
            .unwrap();

        let result = metastore.create_table("users".to_string(), columns);
        assert!(result.is_err());
    }

    #[test]
    fn test_duplicate_column_name() {
        let dir = tempdir().unwrap();
        let metastore = Metastore::new(dir.path()).unwrap();

        let columns = vec![
            ColumnMetadata {
                name: "id".to_string(),
                column_type: ColumnType::Int64,
            },
            ColumnMetadata {
                name: "id".to_string(),
                column_type: ColumnType::Varchar,
            },
        ];

        let result = metastore.create_table("users".to_string(), columns);
        assert!(result.is_err());
    }

    #[test]
    fn test_delete_table() {
        let dir = tempdir().unwrap();
        let metastore = Metastore::new(dir.path()).unwrap();

        let columns = vec![ColumnMetadata {
            name: "id".to_string(),
            column_type: ColumnType::Int64,
        }];

        let table = metastore
            .create_table("users".to_string(), columns)
            .unwrap();
        assert_eq!(metastore.list_tables().len(), 1);

        metastore.delete_table(&table.table_id).unwrap();
        assert!(metastore.list_tables().is_empty());
    }

    #[test]
    fn test_persistence() {
        let dir = tempdir().unwrap();

        {
            let metastore = Metastore::new(dir.path()).unwrap();
            let columns = vec![ColumnMetadata {
                name: "id".to_string(),
                column_type: ColumnType::Int64,
            }];
            metastore
                .create_table("users".to_string(), columns)
                .unwrap();
        }

        // Create new metastore instance - should load persisted data
        let metastore = Metastore::new(dir.path()).unwrap();
        assert_eq!(metastore.list_tables().len(), 1);

        let tables = metastore.list_tables();
        assert_eq!(tables[0].1, "users");
    }

    #[test]
    fn test_get_table_by_name() {
        let dir = tempdir().unwrap();
        let metastore = Metastore::new(dir.path()).unwrap();

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

        let created = metastore
            .create_table("products".to_string(), columns)
            .unwrap();

        let found = metastore.get_table_by_name("products").unwrap();
        assert_eq!(found.table_id, created.table_id);
        assert_eq!(found.name, "products");
        assert_eq!(found.columns.len(), 2);

        // Non-existent table should return None
        assert!(metastore.get_table_by_name("nonexistent").is_none());
    }

    #[test]
    fn test_table_exists() {
        let dir = tempdir().unwrap();
        let metastore = Metastore::new(dir.path()).unwrap();

        assert!(!metastore.table_exists("users"));

        let columns = vec![ColumnMetadata {
            name: "id".to_string(),
            column_type: ColumnType::Int64,
        }];

        metastore
            .create_table("users".to_string(), columns)
            .unwrap();

        assert!(metastore.table_exists("users"));
        assert!(!metastore.table_exists("products"));
    }

    #[test]
    fn test_add_data_file() {
        let dir = tempdir().unwrap();
        let metastore = Metastore::new(dir.path()).unwrap();

        let columns = vec![ColumnMetadata {
            name: "id".to_string(),
            column_type: ColumnType::Int64,
        }];

        let table = metastore
            .create_table("users".to_string(), columns)
            .unwrap();
        assert!(table.data_files.is_empty());

        // Add data file
        let file_path = dir.path().join("test_data.mimdb");
        metastore
            .add_data_file(&table.table_id, file_path.clone())
            .unwrap();

        // Verify file was added
        let updated_table = metastore.get_table(&table.table_id).unwrap();
        assert_eq!(updated_table.data_files.len(), 1);
        assert_eq!(updated_table.data_files[0], file_path);
    }

    #[test]
    fn test_generate_data_file_path() {
        let dir = tempdir().unwrap();
        let metastore = Metastore::new(dir.path()).unwrap();

        let columns = vec![ColumnMetadata {
            name: "id".to_string(),
            column_type: ColumnType::Int64,
        }];

        let table = metastore
            .create_table("users".to_string(), columns)
            .unwrap();

        let path1 = metastore.generate_data_file_path(&table.table_id);
        let path2 = metastore.generate_data_file_path(&table.table_id);

        // Each call should generate a unique path
        assert_ne!(path1, path2);

        // Path should be under table directory
        assert!(path1.to_str().unwrap().contains(&table.table_id));
        assert!(path1.extension().unwrap() == "mimdb");
    }

    #[test]
    fn test_multiple_tables() {
        let dir = tempdir().unwrap();
        let metastore = Metastore::new(dir.path()).unwrap();

        let columns1 = vec![ColumnMetadata {
            name: "id".to_string(),
            column_type: ColumnType::Int64,
        }];

        let columns2 = vec![
            ColumnMetadata {
                name: "product_id".to_string(),
                column_type: ColumnType::Int64,
            },
            ColumnMetadata {
                name: "name".to_string(),
                column_type: ColumnType::Varchar,
            },
        ];

        metastore
            .create_table("users".to_string(), columns1)
            .unwrap();
        metastore
            .create_table("products".to_string(), columns2)
            .unwrap();

        assert_eq!(metastore.list_tables().len(), 2);
        assert!(metastore.table_exists("users"));
        assert!(metastore.table_exists("products"));
    }

    #[test]
    fn test_delete_nonexistent_table() {
        let dir = tempdir().unwrap();
        let metastore = Metastore::new(dir.path()).unwrap();

        let result = metastore.delete_table("nonexistent-id");
        assert!(result.is_err());
    }

    #[test]
    fn test_add_data_file_to_nonexistent_table() {
        let dir = tempdir().unwrap();
        let metastore = Metastore::new(dir.path()).unwrap();

        let result = metastore.add_data_file("nonexistent-id", dir.path().join("file.mimdb"));
        assert!(result.is_err());
    }

    #[test]
    fn test_persistence_with_data_files() {
        let dir = tempdir().unwrap();

        let table_id = {
            let metastore = Metastore::new(dir.path()).unwrap();
            let columns = vec![ColumnMetadata {
                name: "id".to_string(),
                column_type: ColumnType::Int64,
            }];

            let table = metastore
                .create_table("users".to_string(), columns)
                .unwrap();

            // Add some data files
            metastore
                .add_data_file(&table.table_id, dir.path().join("file1.mimdb"))
                .unwrap();
            metastore
                .add_data_file(&table.table_id, dir.path().join("file2.mimdb"))
                .unwrap();

            table.table_id
        };

        // Load metastore again and verify data files persisted
        let metastore = Metastore::new(dir.path()).unwrap();
        let table = metastore.get_table(&table_id).unwrap();

        assert_eq!(table.data_files.len(), 2);
    }

    #[test]
    fn test_table_access_tracking() {
        let dir = tempdir().unwrap();
        let metastore = Metastore::new(dir.path()).unwrap();

        let columns = vec![ColumnMetadata {
            name: "id".to_string(),
            column_type: ColumnType::Int64,
        }];

        let table = metastore
            .create_table("users".to_string(), columns)
            .unwrap();

        // Initially no accesses
        assert_eq!(metastore.active_access_count(&table.table_id), 0);

        // Acquire access
        metastore
            .acquire_table_access(&table.table_id, "query1")
            .unwrap();
        assert_eq!(metastore.active_access_count(&table.table_id), 1);

        // Acquire another access
        metastore
            .acquire_table_access(&table.table_id, "query2")
            .unwrap();
        assert_eq!(metastore.active_access_count(&table.table_id), 2);

        // Release one access
        metastore.release_table_access(&table.table_id, "query1");
        assert_eq!(metastore.active_access_count(&table.table_id), 1);

        // Release last access
        metastore.release_table_access(&table.table_id, "query2");
        assert_eq!(metastore.active_access_count(&table.table_id), 0);
    }

    #[test]
    fn test_delete_table_with_active_queries_defers_file_deletion() {
        let dir = tempdir().unwrap();
        let metastore = Metastore::new(dir.path()).unwrap();

        let columns = vec![ColumnMetadata {
            name: "id".to_string(),
            column_type: ColumnType::Int64,
        }];

        let table = metastore
            .create_table("users".to_string(), columns)
            .unwrap();
        let table_id = table.table_id.clone();

        // Create a real data file
        let data_file = metastore.generate_data_file_path(&table_id);
        fs::create_dir_all(data_file.parent().unwrap()).unwrap();
        fs::write(&data_file, b"test data").unwrap();
        metastore
            .add_data_file(&table_id, data_file.clone())
            .unwrap();

        // Acquire table access (simulating an active query)
        metastore.acquire_table_access(&table_id, "query1").unwrap();

        // Delete the table - should not delete files yet
        metastore.delete_table(&table_id).unwrap();

        // Table should be gone from listing
        assert!(metastore.list_tables().is_empty());
        assert!(metastore.get_table(&table_id).is_none());

        // But the file should still exist
        assert!(
            data_file.exists(),
            "File should still exist while query is active"
        );

        // Table should be pending deletion
        assert!(metastore.is_pending_deletion(&table_id));

        // Release the access - should trigger cleanup
        metastore.release_table_access(&table_id, "query1");

        // Now the file should be deleted
        assert!(
            !data_file.exists(),
            "File should be deleted after query completes"
        );
        assert!(!metastore.is_pending_deletion(&table_id));
    }

    #[test]
    fn test_delete_table_without_active_queries_deletes_files_immediately() {
        let dir = tempdir().unwrap();
        let metastore = Metastore::new(dir.path()).unwrap();

        let columns = vec![ColumnMetadata {
            name: "id".to_string(),
            column_type: ColumnType::Int64,
        }];

        let table = metastore
            .create_table("users".to_string(), columns)
            .unwrap();
        let table_id = table.table_id.clone();

        // Create a real data file
        let data_file = metastore.generate_data_file_path(&table_id);
        fs::create_dir_all(data_file.parent().unwrap()).unwrap();
        fs::write(&data_file, b"test data").unwrap();
        metastore
            .add_data_file(&table_id, data_file.clone())
            .unwrap();

        // Delete the table without any active queries
        metastore.delete_table(&table_id).unwrap();

        // File should be deleted immediately
        assert!(
            !data_file.exists(),
            "File should be deleted immediately when no active queries"
        );
        assert!(!metastore.is_pending_deletion(&table_id));
    }

    #[test]
    fn test_pending_deletions_cleaned_on_restart() {
        let dir = tempdir().unwrap();
        let table_id;
        let data_file;

        {
            let metastore = Metastore::new(dir.path()).unwrap();

            let columns = vec![ColumnMetadata {
                name: "id".to_string(),
                column_type: ColumnType::Int64,
            }];

            let table = metastore
                .create_table("users".to_string(), columns)
                .unwrap();
            table_id = table.table_id.clone();

            // Create a real data file
            data_file = metastore.generate_data_file_path(&table_id);
            fs::create_dir_all(data_file.parent().unwrap()).unwrap();
            fs::write(&data_file, b"test data").unwrap();
            metastore
                .add_data_file(&table_id, data_file.clone())
                .unwrap();

            // Acquire access and delete (simulating crash with active query)
            metastore.acquire_table_access(&table_id, "query1").unwrap();
            metastore.delete_table(&table_id).unwrap();

            // File should still exist (query active)
            assert!(data_file.exists());
            assert!(metastore.is_pending_deletion(&table_id));

            // Don't release - simulate crash by dropping without release
        }

        // On restart, pending deletions should be cleaned up
        // (no active queries in fresh metastore)
        let metastore = Metastore::new(dir.path()).unwrap();

        // File should now be deleted
        assert!(
            !data_file.exists(),
            "File should be deleted on restart when no active queries"
        );
        assert!(!metastore.is_pending_deletion(&table_id));
    }

    #[test]
    fn test_acquire_access_to_deleted_table_fails() {
        let dir = tempdir().unwrap();
        let metastore = Metastore::new(dir.path()).unwrap();

        let columns = vec![ColumnMetadata {
            name: "id".to_string(),
            column_type: ColumnType::Int64,
        }];

        let table = metastore
            .create_table("users".to_string(), columns)
            .unwrap();
        let table_id = table.table_id.clone();

        // Delete the table
        metastore.delete_table(&table_id).unwrap();

        // Trying to acquire access should fail
        let result = metastore.acquire_table_access(&table_id, "query1");
        assert!(result.is_err());
    }
}
