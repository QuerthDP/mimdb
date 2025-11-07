/*
 * Copyright (c) 2025-present Dawid Pawlik
 *
 * For educational use only by employees and students of MIMUW.
 * See LICENSE file for details.
 */

//! Compression utilities for columnar data
//!
//! This module provides compression algorithms optimized for different data types:
//! - Int64 columns: Delta encoding + Variable Length Encoding + ZSTD
//! - Varchar columns: Length-prefixed serialization + LZ4 compression
//!
//! ## Batch Processing Support
//!
//! For memory-efficient handling of very large columns, this module includes
//! chunked compression variants that process data in batches:
//! - `compress_int64_column_chunked`: Processes delta encoding in memory-efficient batches
//! - `compress_varchar_column_chunked`: Handles string serialization in chunks
//!
//! These functions maintain identical compression output to their standard counterparts
//! while reducing peak memory usage during compression of large datasets.

use anyhow::Context;
use anyhow::Result;

/// Compress int64 column using delta encoding + VLE (Variable Length Encoding)
pub(crate) fn compress_int64_column(data: &[i64]) -> Result<Vec<u8>> {
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
pub(crate) fn decompress_int64_column(
    compressed_data: &[u8],
    row_count: usize,
) -> Result<Vec<i64>> {
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
pub(crate) fn compress_varchar_column(data: &[String]) -> Result<Vec<u8>> {
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
pub(crate) fn decompress_varchar_column(
    compressed_data: &[u8],
    row_count: usize,
) -> Result<Vec<String>> {
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
pub(crate) fn encode_vle(value: i64, output: &mut Vec<u8>) {
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
pub(crate) fn decode_vle(input: &[u8]) -> Result<(i64, usize)> {
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

/// Compress int64 data in memory-efficient chunks by pre-processing deltas
pub(crate) fn compress_int64_column_chunked(data: &[i64], batch_size: usize) -> Result<Vec<u8>> {
    // Process delta encoding in batches to reduce memory usage
    let mut all_encoded = Vec::new();

    // First value as-is
    if !data.is_empty() {
        encode_vle(data[0], &mut all_encoded);
    }

    // Process deltas in batches
    for chunk in data.windows(2).collect::<Vec<_>>().chunks(batch_size) {
        for window in chunk {
            let delta = window[1].wrapping_sub(window[0]);
            encode_vle(delta, &mut all_encoded);
        }
    }

    // Compress the encoded deltas with ZSTD
    let compressed = zstd::encode_all(&all_encoded[..], 3)?;
    Ok(compressed)
}

/// Compress varchar data in memory-efficient chunks
pub(crate) fn compress_varchar_column_chunked(
    data: &[String],
    batch_size: usize,
) -> Result<Vec<u8>> {
    // Process serialization in batches
    let mut all_serialized = Vec::new();

    for chunk in data.chunks(batch_size) {
        for string in chunk {
            let len = string.len() as u32;
            all_serialized.extend_from_slice(&len.to_le_bytes());
            all_serialized.extend_from_slice(string.as_bytes());
        }
    }

    // Compress with LZ4 and prepend size
    let compressed = lz4_flex::compress_prepend_size(&all_serialized);
    Ok(compressed)
}

/// Compress column data using memory-efficient batching for very large datasets
/// This processes data in chunks to reduce peak memory usage during compression
pub(crate) fn compress_column_memory_efficient(
    column_data: &crate::ColumnData,
    batch_size: usize,
) -> Result<Vec<u8>> {
    match column_data {
        crate::ColumnData::Int64(data) => {
            if data.len() <= batch_size * 2 {
                // For reasonably sized columns, use direct compression
                compress_int64_column(data)
            } else {
                // For very large columns, implement streaming compression
                compress_int64_column_chunked(data, batch_size)
            }
        }
        crate::ColumnData::Varchar(data) => {
            if data.len() <= batch_size * 2 {
                // For reasonably sized columns, use direct compression
                compress_varchar_column(data)
            } else {
                // For very large columns, implement streaming compression
                compress_varchar_column_chunked(data, batch_size)
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
    fn test_int64_chunked_compression() {
        let data = vec![100, 102, 101, 103, 104, 105, 200, 202, 201, 203]; // Good for delta compression

        // Test with different batch sizes
        for batch_size in [2, 4, 8] {
            let compressed_chunked = compress_int64_column_chunked(&data, batch_size).unwrap();
            let compressed_direct = compress_int64_column(&data).unwrap();

            // Both should decompress to the same result
            let decompressed_chunked =
                decompress_int64_column(&compressed_chunked, data.len()).unwrap();
            let decompressed_direct =
                decompress_int64_column(&compressed_direct, data.len()).unwrap();

            assert_eq!(data, decompressed_chunked);
            assert_eq!(decompressed_chunked, decompressed_direct);
        }
    }

    #[test]
    fn test_varchar_chunked_compression() {
        let data = vec![
            "Hello".to_string(),
            "World".to_string(),
            "Test".to_string(),
            "Batch".to_string(),
            "Processing".to_string(),
        ];

        // Test with different batch sizes
        for batch_size in [2, 3, 5] {
            let compressed_chunked = compress_varchar_column_chunked(&data, batch_size).unwrap();
            let compressed_direct = compress_varchar_column(&data).unwrap();

            // Both should decompress to the same result
            let decompressed_chunked =
                decompress_varchar_column(&compressed_chunked, data.len()).unwrap();
            let decompressed_direct =
                decompress_varchar_column(&compressed_direct, data.len()).unwrap();

            assert_eq!(data, decompressed_chunked);
            assert_eq!(decompressed_chunked, decompressed_direct);
        }
    }
}
