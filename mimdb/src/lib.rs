/*
 * Copyright (c) 2025-present Dawid Pawlik
 *
 * For educational use only by employees and students of MIMUW.
 * See LICENSE file for details.
 */

//! # MIMDB - A Columnar Analytical Database Library
//!
//! This library provides functionality for creating, storing, and loading columnar data tables
//! with efficient compression algorithms optimized for analytical workloads.

use anyhow::Result;
use serde::Deserialize;
use serde::Serialize;
use std::collections::HashMap;

pub mod compression;
pub mod metrics;
pub mod serialization;

/// Column data types supported by the format
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ColumnType {
    Int64,
    Varchar,
}

/// Column metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnMeta {
    pub name: String,
    pub column_type: ColumnType,
    pub compressed_size: usize,
    pub uncompressed_size: usize,
    pub row_count: usize,
}

/// File header structure
#[derive(Debug, Serialize, Deserialize)]
pub struct FileHeader {
    pub version: u32,
    pub column_count: u32,
    pub row_count: u64,
    pub columns: Vec<ColumnMeta>,
}

/// In-memory column data representation optimized for CPU processing
#[derive(Debug, Clone)]
pub enum ColumnData {
    Int64(Vec<i64>),
    Varchar(Vec<String>),
}

impl ColumnData {
    pub fn len(&self) -> usize {
        match self {
            ColumnData::Int64(data) => data.len(),
            ColumnData::Varchar(data) => data.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            ColumnData::Int64(data) => data.is_empty(),
            ColumnData::Varchar(data) => data.is_empty(),
        }
    }

    pub fn column_type(&self) -> ColumnType {
        match self {
            ColumnData::Int64(_) => ColumnType::Int64,
            ColumnData::Varchar(_) => ColumnType::Varchar,
        }
    }
}

/// Main table structure for columnar data
#[derive(Debug)]
pub struct Table {
    pub columns: HashMap<String, ColumnData>,
    pub row_count: usize,
}

impl Default for Table {
    fn default() -> Self {
        Self::new()
    }
}

impl Table {
    pub fn new() -> Self {
        Table {
            columns: HashMap::new(),
            row_count: 0,
        }
    }

    pub fn add_column(&mut self, name: String, data: ColumnData) -> Result<()> {
        if !self.columns.is_empty() && data.len() != self.row_count {
            anyhow::bail!(
                "Column length mismatch: expected {}, got {}",
                self.row_count,
                data.len()
            );
        }

        if self.columns.is_empty() {
            self.row_count = data.len();
        }

        self.columns.insert(name, data);
        Ok(())
    }

    pub fn get_column(&self, name: &str) -> Option<&ColumnData> {
        self.columns.get(name)
    }
}
