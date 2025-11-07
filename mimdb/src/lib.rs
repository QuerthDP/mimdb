/*
 * Copyright (c) 2025-present Dawid Pawlik
 *
 * For educational use only by employees and students of MIMUW.
 * See LICENSE file for details.
 */

//! MIMDB - A Columnar Analytical Database Library
//!
//! This library provides functionality for creating, storing, and loading columnar data tables
//! with efficient compression algorithms optimized for analytical workloads.

use anyhow::Context;
use anyhow::Result;
use serde::Deserialize;
use serde::Serialize;
use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::Read;
use std::io::Write;
use std::path::Path;

// File format constants
const MAGIC_BYTES: &[u8; 8] = b"MIMDB001";
const VERSION: u32 = 1;

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

/// Main table structure for columnar data
#[derive(Debug)]
pub struct Table {
    pub columns: HashMap<String, ColumnData>,
    pub row_count: usize,
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

    /// Serialize table to file with compression
    pub fn save_to_file<P: AsRef<Path>>(&self, path: P) -> Result<()> {
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
    pub fn load_from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
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

/// Compress int64 column using delta encoding + VLE (Variable Length Encoding)
fn compress_int64_column(data: &[i64]) -> Result<Vec<u8>> {
    if data.is_empty() {
        return Ok(Vec::new());
    }

    // Delta encoding
    let mut deltas = Vec::with_capacity(data.len());
    deltas.push(data[0]); // First value as-is

    for i in 1..data.len() {
        deltas.push(data[i].wrapping_sub(data[i - 1]));
    }

    // Variable length encoding
    let mut encoded = Vec::new();
    for &delta in &deltas {
        encode_vle(delta, &mut encoded);
    }

    // Compress with ZSTD
    let compressed = zstd::encode_all(&encoded[..], 3)?;
    Ok(compressed)
}

/// Decompress int64 column
fn decompress_int64_column(compressed_data: &[u8], row_count: usize) -> Result<Vec<i64>> {
    if compressed_data.is_empty() {
        return Ok(Vec::new());
    }

    // Decompress with ZSTD
    let decompressed = zstd::decode_all(compressed_data)?;

    // Decode VLE
    let mut deltas = Vec::with_capacity(row_count);
    let mut pos = 0;

    while pos < decompressed.len() && deltas.len() < row_count {
        let (delta, bytes_read) = decode_vle(&decompressed[pos..])?;
        deltas.push(delta);
        pos += bytes_read;
    }

    // Reconstruct original values from deltas
    let mut result = Vec::with_capacity(row_count);
    if !deltas.is_empty() {
        result.push(deltas[0]);

        for i in 1..deltas.len() {
            let prev = result[i - 1];
            result.push(prev.wrapping_add(deltas[i]));
        }
    }

    Ok(result)
}

/// Compress varchar column using LZ4
fn compress_varchar_column(data: &[String]) -> Result<Vec<u8>> {
    // Serialize strings with length prefixes
    let mut serialized = Vec::new();

    for string in data {
        let len = string.len() as u32;
        serialized.extend_from_slice(&len.to_le_bytes());
        serialized.extend_from_slice(string.as_bytes());
    }

    // Compress with LZ4 and prepend size
    let compressed = lz4_flex::compress_prepend_size(&serialized);
    Ok(compressed)
}

/// Decompress varchar column
fn decompress_varchar_column(compressed_data: &[u8], row_count: usize) -> Result<Vec<String>> {
    if compressed_data.is_empty() {
        return Ok(Vec::new());
    }

    // Decompress with LZ4
    let decompressed = lz4_flex::decompress_size_prepended(compressed_data)
        .map_err(|e| anyhow::anyhow!("LZ4 decompression error: {}", e))?;

    // Deserialize strings
    let mut result = Vec::with_capacity(row_count);
    let mut pos = 0;

    while pos < decompressed.len() && result.len() < row_count {
        if pos + 4 > decompressed.len() {
            break;
        }

        let len = u32::from_le_bytes([
            decompressed[pos],
            decompressed[pos + 1],
            decompressed[pos + 2],
            decompressed[pos + 3],
        ]) as usize;
        pos += 4;

        if pos + len > decompressed.len() {
            break;
        }

        let string_bytes = &decompressed[pos..pos + len];
        let string =
            String::from_utf8(string_bytes.to_vec()).context("Invalid UTF-8 in varchar data")?;

        result.push(string);
        pos += len;
    }

    Ok(result)
}

/// Variable Length Encoding for signed integers
fn encode_vle(value: i64, output: &mut Vec<u8>) {
    // Convert to unsigned using zigzag encoding
    let unsigned = ((value << 1) ^ (value >> 63)) as u64;

    let mut remaining = unsigned;
    while remaining >= 0x80 {
        output.push((remaining & 0x7F) as u8 | 0x80);
        remaining >>= 7;
    }
    output.push(remaining as u8);
}

/// Decode Variable Length Encoded integer
fn decode_vle(input: &[u8]) -> Result<(i64, usize)> {
    let mut result = 0u64;
    let mut shift = 0;
    let mut bytes_read = 0;

    for &byte in input {
        bytes_read += 1;
        result |= ((byte & 0x7F) as u64) << shift;

        if byte & 0x80 == 0 {
            break;
        }

        shift += 7;
        if shift >= 64 {
            anyhow::bail!("VLE integer too large");
        }
    }

    // Convert back from zigzag encoding
    let signed = ((result >> 1) as i64) ^ (-((result & 1) as i64));
    Ok((signed, bytes_read))
}

/// Metrics calculation functions
impl Table {
    /// Calculate average for all integer columns
    pub fn calculate_int_averages(&self) -> HashMap<String, f64> {
        let mut averages = HashMap::new();

        for (name, column) in &self.columns {
            if let ColumnData::Int64(data) = column
                && !data.is_empty()
            {
                let sum: i64 = data.iter().sum();
                let average = sum as f64 / data.len() as f64;
                averages.insert(name.clone(), average);
            }
        }

        averages
    }

    /// Count ASCII characters for all varchar columns
    pub fn calculate_ascii_counts(&self) -> HashMap<String, HashMap<char, usize>> {
        let mut char_counts = HashMap::new();

        for (name, column) in &self.columns {
            if let ColumnData::Varchar(data) = column {
                let mut counts = HashMap::new();

                for string in data {
                    for ch in string.chars() {
                        if ch.is_ascii() {
                            *counts.entry(ch).or_insert(0) += 1;
                        }
                    }
                }

                char_counts.insert(name.clone(), counts);
            }
        }

        char_counts
    }

    /// Get total ASCII character count for a varchar column
    pub fn get_total_ascii_count(&self, column_name: &str) -> Option<usize> {
        if let Some(ColumnData::Varchar(data)) = self.get_column(column_name) {
            let total = data
                .iter()
                .flat_map(|s| s.chars())
                .filter(|&c| c.is_ascii())
                .count();
            Some(total)
        } else {
            None
        }
    }

    /// Print metrics for the table
    pub fn print_metrics(&self) {
        println!("\n=== TABLE METRICS ===");
        println!("Total rows: {}", self.row_count);
        println!("Total columns: {}", self.columns.len());

        // Integer column averages
        let averages = self.calculate_int_averages();
        if !averages.is_empty() {
            println!("\nInteger column averages:");
            for (name, avg) in &averages {
                println!("  {}: {:.4}", name, avg);
            }
        }

        // ASCII character counts for varchar columns
        let char_counts = self.calculate_ascii_counts();
        if !char_counts.is_empty() {
            println!("\nVarchar column ASCII character counts:");
            for name in char_counts.keys() {
                if let Some(total) = self.get_total_ascii_count(name) {
                    println!("  {}: {} total ASCII characters", name, total);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vle_encoding() {
        let test_cases = vec![0, 1, -1, 127, -127, 128, -128, 16383, -16383];

        for value in test_cases {
            let mut encoded = Vec::new();
            encode_vle(value, &mut encoded);
            let (decoded, bytes_read) = decode_vle(&encoded).unwrap();

            assert_eq!(value, decoded);
            assert_eq!(bytes_read, encoded.len());
        }
    }

    #[test]
    fn test_int64_compression() {
        let data = vec![100, 102, 101, 103, 104, 105]; // Good for delta compression

        let compressed = compress_int64_column(&data).unwrap();
        let decompressed = decompress_int64_column(&compressed, data.len()).unwrap();

        assert_eq!(data, decompressed);
    }

    #[test]
    fn test_varchar_compression() {
        let data = vec!["Hello".to_string(), "World".to_string(), "Test".to_string()];

        let compressed = compress_varchar_column(&data).unwrap();
        let decompressed = decompress_varchar_column(&compressed, data.len()).unwrap();

        assert_eq!(data, decompressed);
    }

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
        table.save_to_file(test_file).unwrap();
        let loaded = Table::load_from_file(test_file).unwrap();

        // Verify
        assert_eq!(table.row_count, loaded.row_count);
        assert_eq!(table.columns.len(), loaded.columns.len());

        // Clean up
        std::fs::remove_file(test_file).unwrap();
    }

    #[test]
    fn test_metrics_calculation() {
        let mut table = Table::new();

        table
            .add_column("scores".to_string(), ColumnData::Int64(vec![80, 90, 100]))
            .unwrap();
        table
            .add_column(
                "names".to_string(),
                ColumnData::Varchar(vec![
                    "ABC".to_string(),
                    "DEF".to_string(),
                    "GHI".to_string(),
                ]),
            )
            .unwrap();

        let averages = table.calculate_int_averages();
        assert_eq!(averages.get("scores"), Some(&90.0));

        let ascii_counts = table.calculate_ascii_counts();
        assert!(ascii_counts.contains_key("names"));

        let total_ascii = table.get_total_ascii_count("names").unwrap();
        assert_eq!(total_ascii, 9); // "ABC" + "DEF" + "GHI" = 9 ASCII chars
    }
}
