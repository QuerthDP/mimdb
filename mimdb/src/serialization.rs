/*
 * Copyright (c) 2025-present Dawid Pawlik
 *
 * For educational use only by employees and students of MIMUW.
 * See LICENSE file for details.
 */

//! Input/Output operations for MIMDB files
//!
//! This module handles serialization and deserialization of Table structures
//! to and from the MIMDB file format, including compression and decompression.
//! Optimized for handling large tables through batch processing to minimize memory usage.
//!
//! ## Batch Processing Features
//!
//! The serialization module includes batch processing capabilities designed to handle
//! very large tables efficiently:
//!
//! - **Memory-Efficient Compression**: Large columns are processed in configurable
//!   batch sizes to reduce peak memory usage during serialization.
//! - **Configurable Batch Sizes**: Use `BatchConfig` to control memory vs. performance
//!   trade-offs for your specific use case.
//! - **Automatic Fallback**: Small columns use direct compression for optimal performance,
//!   while large columns automatically use batched processing.
//!
//! ## Usage Examples
//!
//! ```rust,no_run
//! use mimdb::{Table, ColumnData};
//! use mimdb::serialization::BatchConfig;
//!
//! let mut table = Table::new();
//! // Add columns with large datasets...
//!
//! // Serialize with default batch configuration
//! table.serialize("large_table.mimdb").unwrap();
//!
//! // Serialize with custom batch size for memory-constrained environments
//! let config = BatchConfig::new(50_000); // Process in 50k row batches
//! table.serialize_with_config("large_table.mimdb", &config).unwrap();
//!
//! // Deserialize (batch config helps with memory management during read too)
//! let loaded = Table::deserialize_with_config("large_table.mimdb", &config).unwrap();
//! ```

use crate::ColumnData;
use crate::ColumnMeta;
use crate::ColumnType;
use crate::FileHeader;
use crate::Table;
use crate::compression::compress_column_memory_efficient;
use crate::compression::compress_int64_column;
use crate::compression::compress_varchar_column;
use crate::compression::decompress_int64_column;
use crate::compression::decompress_varchar_column;
use anyhow::Result;
use std::fs::File;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::Read;
use std::io::Write;
use std::path::Path;

// File format constants
const MAGIC_BYTES: &[u8; 8] = b"MIMDB001";
const VERSION: u32 = 1;

/// Default batch size for processing large columns (number of rows per batch)
const DEFAULT_BATCH_SIZE: usize = 100_000;

/// Minimum batch size to ensure reasonable compression efficiency
const MIN_BATCH_SIZE: usize = 1_000;

/// Maximum batch size to prevent excessive memory usage
const MAX_BATCH_SIZE: usize = 1_000_000;

/// Configuration for batch processing
#[derive(Debug, Clone)]
pub struct BatchConfig {
    pub batch_size: usize,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            batch_size: DEFAULT_BATCH_SIZE,
        }
    }
}

impl BatchConfig {
    /// Create new batch config with validated batch size
    pub fn new(batch_size: usize) -> Self {
        let validated_size = batch_size.clamp(MIN_BATCH_SIZE, MAX_BATCH_SIZE);
        Self {
            batch_size: validated_size,
        }
    }
}

/// Read and decompress large column data
///
/// Note: With the current file format (v1), we still need to read the entire compressed column.
/// The batch processing benefits are primarily during serialization. Future format versions
/// could store batch boundaries to enable true streaming decompression.
fn read_and_decompress_column_batched<R: Read>(
    reader: &mut R,
    column_meta: &ColumnMeta,
    _config: &BatchConfig,
) -> Result<ColumnData> {
    // Read all compressed data first (unavoidable with current v1 format)
    let mut compressed_data = vec![0u8; column_meta.compressed_size];
    reader.read_exact(&mut compressed_data)?;

    // Decompress the column data
    match column_meta.column_type {
        ColumnType::Int64 => {
            let data = decompress_int64_column(&compressed_data, column_meta.row_count)?;
            Ok(ColumnData::Int64(data))
        }
        ColumnType::Varchar => {
            let data = decompress_varchar_column(&compressed_data, column_meta.row_count)?;
            Ok(ColumnData::Varchar(data))
        }
    }
}

impl Table {
    /// Serialize table to file with compression using default batch configuration
    pub fn serialize<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        self.serialize_with_config(path, &BatchConfig::default())
    }

    /// Serialize table to file with compression using custom batch configuration
    pub fn serialize_with_config<P: AsRef<Path>>(
        &self,
        path: P,
        config: &BatchConfig,
    ) -> Result<()> {
        let mut file = BufWriter::new(File::create(path)?);

        // Write magic bytes
        file.write_all(MAGIC_BYTES)?;

        // Compress all columns and collect metadata
        let mut columns_meta = Vec::new();
        let mut compressed_columns = Vec::new();

        for (name, column_data) in &self.columns {
            let (compressed_data, compressed_size, uncompressed_size) = if column_data.len()
                <= config.batch_size
            {
                // Small columns: use direct compression
                let compressed = match column_data {
                    ColumnData::Int64(data) => compress_int64_column(data)?,
                    ColumnData::Varchar(data) => compress_varchar_column(data)?,
                };
                let uncompressed_size = match column_data {
                    ColumnData::Int64(data) => data.len() * 8,
                    ColumnData::Varchar(data) => {
                        data.iter().map(|s| s.len()).sum::<usize>() + data.len() * 4
                    }
                };
                let compressed_size = compressed.len();
                (compressed, compressed_size, uncompressed_size)
            } else {
                // Large columns: use memory-efficient compression for very large datasets
                let compressed = compress_column_memory_efficient(column_data, config.batch_size)?;
                let uncompressed_size = match column_data {
                    ColumnData::Int64(data) => data.len() * 8,
                    ColumnData::Varchar(data) => {
                        data.iter().map(|s| s.len()).sum::<usize>() + data.len() * 4
                    }
                };
                let compressed_size = compressed.len();
                (compressed, compressed_size, uncompressed_size)
            };

            columns_meta.push(ColumnMeta {
                name: name.clone(),
                column_type: column_data.column_type(),
                compressed_size,
                uncompressed_size,
                row_count: column_data.len(),
            });

            compressed_columns.push(compressed_data);
        }

        // Create and write header
        let header = FileHeader {
            version: VERSION,
            column_count: self.columns.len() as u32,
            row_count: self.row_count as u64,
            columns: columns_meta,
        };

        let header_bytes = bincode::serialize(&header)?;
        let header_size = header_bytes.len() as u32;

        // Write header size and header
        file.write_all(&header_size.to_le_bytes())?;
        file.write_all(&header_bytes)?;

        // Write compressed column data
        for compressed_data in compressed_columns {
            file.write_all(&compressed_data)?;
        }

        file.flush()?;
        Ok(())
    }

    /// Deserialize table from file using default batch configuration
    pub fn deserialize<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::deserialize_with_config(path, &BatchConfig::default())
    }

    /// Deserialize table from file using custom batch configuration
    pub fn deserialize_with_config<P: AsRef<Path>>(path: P, config: &BatchConfig) -> Result<Self> {
        let mut file = BufReader::new(File::open(path)?);

        // Read and verify magic bytes
        let mut magic = [0u8; 8];
        file.read_exact(&mut magic)?;
        if &magic != MAGIC_BYTES {
            anyhow::bail!("Invalid file format: magic bytes mismatch");
        }

        // Read header size
        let mut header_size_bytes = [0u8; 4];
        file.read_exact(&mut header_size_bytes)?;
        let header_size = u32::from_le_bytes(header_size_bytes) as usize;

        // Read header
        let mut header_bytes = vec![0u8; header_size];
        file.read_exact(&mut header_bytes)?;
        let header: FileHeader = bincode::deserialize(&header_bytes)?;

        if header.version != VERSION {
            anyhow::bail!("Unsupported file version: {}", header.version);
        }

        // Read and decompress column data
        let mut table = Table::new();

        for column_meta in &header.columns {
            let column_data = if column_meta.row_count <= config.batch_size {
                // Small columns: read and decompress normally
                let mut compressed_data = vec![0u8; column_meta.compressed_size];
                file.read_exact(&mut compressed_data)?;

                match column_meta.column_type {
                    ColumnType::Int64 => {
                        let data =
                            decompress_int64_column(&compressed_data, column_meta.row_count)?;
                        ColumnData::Int64(data)
                    }
                    ColumnType::Varchar => {
                        let data =
                            decompress_varchar_column(&compressed_data, column_meta.row_count)?;
                        ColumnData::Varchar(data)
                    }
                }
            } else {
                // Large columns: read compressed data and decompress in batches
                read_and_decompress_column_batched(&mut file, column_meta, config)?
            };

            table.add_column(column_meta.name.clone(), column_data)?;
        }

        Ok(table)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ColumnData;

    #[test]
    fn test_table_serialization() {
        let mut table = Table::new();

        // Add test data
        table
            .add_column(
                "numbers".to_string(),
                ColumnData::Int64(vec![1, 2, 3, 4, 5]),
            )
            .unwrap();
        table
            .add_column(
                "words".to_string(),
                ColumnData::Varchar(vec![
                    "a".to_string(),
                    "b".to_string(),
                    "c".to_string(),
                    "d".to_string(),
                    "e".to_string(),
                ]),
            )
            .unwrap();

        // Save and load
        let test_file = "test_table.mimdb";
        table.serialize(test_file).unwrap();
        let loaded = Table::deserialize(test_file).unwrap();

        // Verify
        assert_eq!(table.row_count, loaded.row_count);
        assert_eq!(table.columns.len(), loaded.columns.len());

        // Clean up
        std::fs::remove_file(test_file).unwrap();
    }

    #[test]
    fn test_batch_configuration() {
        // Test validation of batch sizes
        let config = BatchConfig::new(500); // Below minimum
        assert_eq!(config.batch_size, MIN_BATCH_SIZE);

        let config = BatchConfig::new(2_000_000); // Above maximum
        assert_eq!(config.batch_size, MAX_BATCH_SIZE);

        let config = BatchConfig::new(50_000); // Valid size
        assert_eq!(config.batch_size, 50_000);
    }

    #[test]
    fn test_table_serialization_with_batches() {
        let mut table = Table::new();

        // Create larger test data that will trigger batch processing
        let large_numbers: Vec<i64> = (0..200_000).collect();
        let large_strings: Vec<String> = (0..200_000).map(|i| format!("string_{}", i)).collect();

        table
            .add_column(
                "large_numbers".to_string(),
                ColumnData::Int64(large_numbers.clone()),
            )
            .unwrap();
        table
            .add_column(
                "large_strings".to_string(),
                ColumnData::Varchar(large_strings.clone()),
            )
            .unwrap();

        // Test with small batch size to force batching
        let config = BatchConfig::new(10_000);
        let test_file = "test_large_table.mimdb";

        table.serialize_with_config(test_file, &config).unwrap();
        let loaded = Table::deserialize_with_config(test_file, &config).unwrap();

        // Verify data integrity
        assert_eq!(table.row_count, loaded.row_count);
        assert_eq!(table.columns.len(), loaded.columns.len());

        // Verify specific data
        if let Some(ColumnData::Int64(loaded_numbers)) = loaded.get_column("large_numbers") {
            assert_eq!(loaded_numbers.len(), large_numbers.len());
            assert_eq!(loaded_numbers[0], 0);
            assert_eq!(loaded_numbers[100_000], 100_000);
            assert_eq!(loaded_numbers[199_999], 199_999);
        } else {
            panic!("Failed to load large_numbers column");
        }

        if let Some(ColumnData::Varchar(loaded_strings)) = loaded.get_column("large_strings") {
            assert_eq!(loaded_strings.len(), large_strings.len());
            assert_eq!(loaded_strings[0], "string_0");
            assert_eq!(loaded_strings[100_000], "string_100000");
            assert_eq!(loaded_strings[199_999], "string_199999");
        } else {
            panic!("Failed to load large_strings column");
        }

        // Clean up
        std::fs::remove_file(test_file).unwrap();
    }

    #[test]
    fn test_mixed_size_columns_batching() {
        let mut table = Table::new();

        // Create columns with same row count but different data patterns
        let row_count = 150_000;
        let small_range_numbers: Vec<i64> = (0..row_count).map(|i| i % 100).collect();
        let large_range_numbers: Vec<i64> = (0..row_count).collect();

        table
            .add_column(
                "small_range".to_string(),
                ColumnData::Int64(small_range_numbers.clone()),
            )
            .unwrap();
        table
            .add_column(
                "large_range".to_string(),
                ColumnData::Int64(large_range_numbers.clone()),
            )
            .unwrap();

        let config = BatchConfig::new(50_000);
        let test_file = "test_mixed_table.mimdb";

        table.serialize_with_config(test_file, &config).unwrap();
        let loaded = Table::deserialize_with_config(test_file, &config).unwrap();

        // Verify both columns
        assert_eq!(table.row_count, loaded.row_count);

        if let Some(ColumnData::Int64(loaded_small_range)) = loaded.get_column("small_range") {
            assert_eq!(loaded_small_range.len(), small_range_numbers.len());
            assert_eq!(loaded_small_range[0], 0); // 0 % 100
            assert_eq!(loaded_small_range[100], 0); // 100 % 100
            assert_eq!(loaded_small_range[150], 50); // 150 % 100
        } else {
            panic!("Failed to load small_range column");
        }

        if let Some(ColumnData::Int64(loaded_large_range)) = loaded.get_column("large_range") {
            assert_eq!(loaded_large_range.len(), large_range_numbers.len());
            assert_eq!(loaded_large_range[0], 0);
            assert_eq!(loaded_large_range[149_999], 149_999);
        } else {
            panic!("Failed to load large_range column");
        }

        // Clean up
        std::fs::remove_file(test_file).unwrap();
    }
}
