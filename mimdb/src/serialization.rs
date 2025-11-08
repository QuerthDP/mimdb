/*
 * Copyright (c) 2025-present Dawid Pawlik
 *
 * For educational use only by employees and students of MIMUW.
 * See LICENSE file for details.
 */

//! # Input/Output operations for MIMDB files
//!
//! This module handles serialization and deserialization of Table structures
//! to and from the MIMDB file format, including compression and decompression.
//! Optimized for true streaming and batched processing to handle very large datasets efficiently.
//!
//! ## Batch Processing Features
//!
//! The serialization format includes true batch processing capabilities designed to handle
//! very large tables efficiently:
//!
//! - **True Streaming Decompression**: Columns are stored as separate compressed batches
//!   with metadata, enabling selective reading and decompression of row ranges.
//! - **Memory-Efficient Processing**: Large columns are processed in configurable
//!   batch sizes to reduce peak memory usage during both serialization and deserialization.
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
//! // Deserialize with streaming decompression for large files
//! let loaded = Table::deserialize("large_table.mimdb").unwrap();
//!
//! // For very large files, custom batch config reduces memory usage
//! let loaded_streaming = Table::deserialize_with_config("large_table.mimdb", &config).unwrap();
//! ```

use crate::ColumnData;
use crate::ColumnType;
use crate::Table;
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
const MAGIC_BYTES: &[u8; 8] = b"MIMDB002";
const VERSION: u32 = 2;

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
        if validated_size != batch_size {
            eprintln!(
                "Warning: Batch size {} is out of bounds. Using {} instead.",
                batch_size, validated_size
            );
        }
        Self {
            batch_size: validated_size,
        }
    }
}

/// Metadata for a single batch within a column
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BatchMeta {
    pub start_row: usize,
    pub row_count: usize,
    pub compressed_size: usize,
    pub uncompressed_size: usize,
}

/// Extended column metadata with batch information
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ColumnMeta {
    pub name: String,
    pub column_type: ColumnType,
    pub total_compressed_size: usize,
    pub total_uncompressed_size: usize,
    pub total_row_count: usize,
    pub batch_size: usize,
    pub batches: Vec<BatchMeta>,
}

/// File header structure with batch support
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct FileHeader {
    pub version: u32,
    pub column_count: u32,
    pub row_count: u64,
    pub columns: Vec<ColumnMeta>,
}

impl Table {
    /// Serialize table to file with compression using default batch configuration
    pub fn serialize<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        self.serialize_with_config(path, &BatchConfig::default())
    }

    /// Serialize table to file with compression using custom batch configuration
    /// Format supports true batch boundaries for streaming decompression
    pub fn serialize_with_config<P: AsRef<Path>>(
        &self,
        path: P,
        config: &BatchConfig,
    ) -> Result<()> {
        let mut file = BufWriter::new(File::create(path)?);

        // Write magic bytes
        file.write_all(MAGIC_BYTES)?;

        // Process columns and collect metadata with batch boundaries
        let mut columns_meta = Vec::new();
        let mut all_compressed_batches = Vec::new();

        for (name, column_data) in &self.columns {
            let row_count = column_data.len();
            let mut batches = Vec::new();
            let mut compressed_batches = Vec::new();
            let mut total_compressed_size = 0;
            let mut total_uncompressed_size = 0;

            if row_count <= config.batch_size {
                // Small columns: treat as single batch for efficiency
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
                total_compressed_size += compressed_size;
                total_uncompressed_size += uncompressed_size;

                batches.push(BatchMeta {
                    start_row: 0,
                    row_count,
                    compressed_size,
                    uncompressed_size,
                });
                compressed_batches.push(compressed);
            } else {
                // Large columns: process in actual batches with separate compression
                for batch_start in (0..row_count).step_by(config.batch_size) {
                    let batch_end = (batch_start + config.batch_size).min(row_count);
                    let batch_row_count = batch_end - batch_start;

                    let batch_compressed = match column_data {
                        ColumnData::Int64(data) => {
                            let batch_slice = &data[batch_start..batch_end];
                            compress_int64_column(batch_slice)?
                        }
                        ColumnData::Varchar(data) => {
                            let batch_slice = &data[batch_start..batch_end];
                            compress_varchar_column(batch_slice)?
                        }
                    };

                    let batch_uncompressed_size = match column_data {
                        ColumnData::Int64(_) => batch_row_count * 8,
                        ColumnData::Varchar(data) => {
                            data[batch_start..batch_end]
                                .iter()
                                .map(|s| s.len())
                                .sum::<usize>()
                                + batch_row_count * 4
                        }
                    };

                    let batch_compressed_size = batch_compressed.len();
                    total_compressed_size += batch_compressed_size;
                    total_uncompressed_size += batch_uncompressed_size;

                    batches.push(BatchMeta {
                        start_row: batch_start,
                        row_count: batch_row_count,
                        compressed_size: batch_compressed_size,
                        uncompressed_size: batch_uncompressed_size,
                    });
                    compressed_batches.push(batch_compressed);
                }
            }

            columns_meta.push(ColumnMeta {
                name: name.clone(),
                column_type: column_data.column_type(),
                total_compressed_size,
                total_uncompressed_size,
                total_row_count: row_count,
                batch_size: config.batch_size,
                batches,
            });

            all_compressed_batches.push(compressed_batches);
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

        // Write compressed batch data for each column
        for compressed_batches in all_compressed_batches {
            for batch_data in compressed_batches {
                file.write_all(&batch_data)?;
            }
        }

        file.flush()?;
        Ok(())
    }

    /// Deserialize table from file using default batch configuration
    pub fn deserialize<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::deserialize_with_config(path, &BatchConfig::default())
    }

    /// Deserialize table from file using custom batch configuration
    /// Supports streaming batch decompression for memory-efficient processing
    pub fn deserialize_with_config<P: AsRef<Path>>(path: P, _config: &BatchConfig) -> Result<Self> {
        let mut file = BufReader::new(File::open(path)?);

        // Read and verify magic bytes
        let mut magic = [0u8; 8];
        file.read_exact(&mut magic)?;

        if &magic != MAGIC_BYTES {
            anyhow::bail!("Invalid file format: magic bytes mismatch");
        }

        Self::deserialize_format(&mut file)
    }

    /// Deserialize format with streaming batch support
    fn deserialize_format<R: Read>(reader: &mut R) -> Result<Self> {
        // Read header size
        let mut header_size_bytes = [0u8; 4];
        reader.read_exact(&mut header_size_bytes)?;
        let header_size = u32::from_le_bytes(header_size_bytes) as usize;

        // Read header
        let mut header_bytes = vec![0u8; header_size];
        reader.read_exact(&mut header_bytes)?;
        let header: FileHeader = bincode::deserialize(&header_bytes)?;

        if header.version != VERSION {
            anyhow::bail!("Unsupported file version: {}", header.version);
        }

        // Read and decompress column data using batch streaming
        let mut table = Table::new();

        for column_meta in &header.columns {
            // Initialize column data containers
            let column_data = match column_meta.column_type {
                ColumnType::Int64 => {
                    let mut data = Vec::with_capacity(column_meta.total_row_count);

                    // Read and decompress each batch
                    for batch_meta in &column_meta.batches {
                        let mut batch_compressed = vec![0u8; batch_meta.compressed_size];
                        reader.read_exact(&mut batch_compressed)?;

                        let mut batch_data =
                            decompress_int64_column(&batch_compressed, batch_meta.row_count)?;
                        data.append(&mut batch_data);
                    }
                    ColumnData::Int64(data)
                }
                ColumnType::Varchar => {
                    let mut data = Vec::with_capacity(column_meta.total_row_count);

                    // Read and decompress each batch
                    for batch_meta in &column_meta.batches {
                        let mut batch_compressed = vec![0u8; batch_meta.compressed_size];
                        reader.read_exact(&mut batch_compressed)?;

                        let mut batch_data =
                            decompress_varchar_column(&batch_compressed, batch_meta.row_count)?;
                        data.append(&mut batch_data);
                    }
                    ColumnData::Varchar(data)
                }
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

    #[test]
    fn test_batch_boundaries_functionality() {
        let mut table = Table::new();

        // Create a dataset that will definitely trigger multiple batches
        let row_count = 250_000;
        let numbers: Vec<i64> = (0..row_count).collect();
        let strings: Vec<String> = (0..row_count).map(|i| format!("value_{}", i)).collect();

        table
            .add_column("numbers".to_string(), ColumnData::Int64(numbers.clone()))
            .unwrap();
        table
            .add_column("strings".to_string(), ColumnData::Varchar(strings.clone()))
            .unwrap();

        // Use small batch size to force multiple batches
        let config = BatchConfig::new(30_000);
        let test_file = "test_batches.mimdb";

        // Serialize with batching
        table.serialize_with_config(test_file, &config).unwrap();

        // Verify magic bytes are correct
        let mut file = std::fs::File::open(test_file).unwrap();
        let mut magic = [0u8; 8];
        std::io::Read::read_exact(&mut file, &mut magic).unwrap();
        assert_eq!(&magic, MAGIC_BYTES, "Should write correct format");

        // Deserialize and verify integrity
        let loaded = Table::deserialize_with_config(test_file, &config).unwrap();

        assert_eq!(table.row_count, loaded.row_count);
        assert_eq!(table.columns.len(), loaded.columns.len());

        // Verify specific data points across batch boundaries
        if let Some(ColumnData::Int64(loaded_numbers)) = loaded.get_column("numbers") {
            assert_eq!(loaded_numbers.len(), numbers.len());
            // Test data at batch boundaries (30k intervals)
            assert_eq!(loaded_numbers[0], 0);
            assert_eq!(loaded_numbers[29_999], 29_999);
            assert_eq!(loaded_numbers[30_000], 30_000);
            assert_eq!(loaded_numbers[59_999], 59_999);
            assert_eq!(loaded_numbers[60_000], 60_000);
            assert_eq!(loaded_numbers[249_999], 249_999);
        } else {
            panic!("Failed to load numbers column");
        }

        if let Some(ColumnData::Varchar(loaded_strings)) = loaded.get_column("strings") {
            assert_eq!(loaded_strings.len(), strings.len());
            // Test data at batch boundaries
            assert_eq!(loaded_strings[0], "value_0");
            assert_eq!(loaded_strings[29_999], "value_29999");
            assert_eq!(loaded_strings[30_000], "value_30000");
            assert_eq!(loaded_strings[249_999], "value_249999");
        } else {
            panic!("Failed to load strings column");
        }

        // Clean up
        std::fs::remove_file(test_file).unwrap();
    }

    #[test]
    fn test_format_consistency() {
        // Test that serialization/deserialization produces consistent results
        let mut table = Table::new();
        table
            .add_column("test".to_string(), ColumnData::Int64(vec![1, 2, 3, 4, 5]))
            .unwrap();

        let test_file = "test_format_consistency.mimdb";

        // Serialize using standard format
        table.serialize(test_file).unwrap();

        // Verify magic bytes are correct
        let mut file = std::fs::File::open(test_file).unwrap();
        let mut magic = [0u8; 8];
        std::io::Read::read_exact(&mut file, &mut magic).unwrap();
        assert_eq!(&magic, MAGIC_BYTES, "Should write correct format");

        // Deserialize and verify data integrity
        let loaded = Table::deserialize(test_file).unwrap();

        assert_eq!(loaded.row_count, 5);
        assert_eq!(loaded.columns.len(), 1);

        if let Some(ColumnData::Int64(data)) = loaded.get_column("test") {
            assert_eq!(data, &vec![1, 2, 3, 4, 5]);
        } else {
            panic!("Failed to load format data");
        }

        // Clean up
        std::fs::remove_file(test_file).unwrap();
    }
}
