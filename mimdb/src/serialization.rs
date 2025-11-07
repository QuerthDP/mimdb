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

use crate::ColumnData;
use crate::ColumnMeta;
use crate::ColumnType;
use crate::FileHeader;
use crate::MAGIC_BYTES;
use crate::Table;
use crate::VERSION;
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

impl Table {
    /// Serialize table to file with compression
    pub fn serialize<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let mut file = BufWriter::new(File::create(path)?);

        // Write magic bytes
        file.write_all(MAGIC_BYTES)?;

        // Prepare column metadata and compressed data
        let mut columns_meta = Vec::new();
        let mut compressed_data = Vec::new();

        for (name, column_data) in &self.columns {
            let (compressed, uncompressed_size) = match column_data {
                ColumnData::Int64(data) => {
                    let compressed = compress_int64_column(data)?;
                    (compressed, data.len() * 8) // 8 bytes per i64
                }
                ColumnData::Varchar(data) => {
                    let compressed = compress_varchar_column(data)?;
                    let uncompressed_size =
                        data.iter().map(|s| s.len()).sum::<usize>() + data.len() * 4; // string lengths
                    (compressed, uncompressed_size)
                }
            };

            columns_meta.push(ColumnMeta {
                name: name.clone(),
                column_type: column_data.column_type(),
                compressed_size: compressed.len(),
                uncompressed_size,
                row_count: column_data.len(),
            });

            compressed_data.push(compressed);
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
        for data in compressed_data {
            file.write_all(&data)?;
        }

        file.flush()?;
        Ok(())
    }

    /// Deserialize table from file
    pub fn deserialize<P: AsRef<Path>>(path: P) -> Result<Self> {
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
            let mut compressed_data = vec![0u8; column_meta.compressed_size];
            file.read_exact(&mut compressed_data)?;

            let column_data = match column_meta.column_type {
                ColumnType::Int64 => {
                    let data = decompress_int64_column(&compressed_data, column_meta.row_count)?;
                    ColumnData::Int64(data)
                }
                ColumnType::Varchar => {
                    let data = decompress_varchar_column(&compressed_data, column_meta.row_count)?;
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
}
