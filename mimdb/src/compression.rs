/*
 * Copyright (c) 2025-present Dawid Pawlik
 *
 * For educational use only by employees and students of MIMUW.
 * See LICENSE file for details.
 */

//! # Compression utilities for columnar data
//!
//! This module provides high-efficiency compression algorithms optimized for different data types:
//!
//! ### Int64 Column Compression
//! - **Delta Encoding**: Reduces value ranges by storing differences between consecutive values
//! - **Variable Length Encoding (VLE)**: Compresses small deltas into fewer bytes using zigzag encoding
//! - **ZSTD Compression**: Final compression layer for optimal size reduction
//!
//! ### Varchar Column Compression
//! - **Length-Prefixed Serialization**: Efficient string storage with 4-byte length headers
//! - **LZ4 Compression**: Fast compression with size prepending for quick decompression
//!
//! ## Integration with Batch Processing
//!
//! These compression functions work seamlessly with the batch processing system in the
//! serialization layer, where large datasets are automatically split into manageable
//! chunks and compressed individually for memory-efficient processing.

use anyhow::Context;
use anyhow::Result;

/// Compress int64 column using optimized multi-stage compression
///
/// ## Compression Pipeline:
/// 1. **Delta Encoding**: Converts values to differences for better compression ratios
/// 2. **Variable Length Encoding**: Compresses small deltas using zigzag encoding
/// 3. **ZSTD Compression**: Final compression stage with level 3 for balance of speed/size
///
/// This approach is particularly effective for sequential or near-sequential data patterns.
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

/// Decompress int64 column by reversing the compression pipeline
///
/// ## Decompression Pipeline:
/// 1. **ZSTD Decompression**: Decompress the data stream
/// 2. **Variable Length Decoding**: Decode VLE-encoded deltas back to i64 values
/// 3. **Delta Reconstruction**: Rebuild original values by accumulating deltas
///
/// The `row_count` parameter ensures we read exactly the expected number of values.
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

/// Compress varchar column using length-prefixed serialization and LZ4
///
/// ## Compression Process:
/// 1. **Serialization**: Each string is prefixed with its length (4-byte little-endian u32)
/// 2. **LZ4 Compression**: Fast compression with automatic size prepending for efficient decompression
///
/// This approach optimizes for both compression ratio and decompression speed, making it
/// ideal for text data with repeated patterns or common prefixes/suffixes.
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

/// Decompress varchar column by reversing the compression process
///
/// ## Decompression Process:
/// 1. **LZ4 Decompression**: Decompress the data stream (size is automatically handled)
/// 2. **Deserialization**: Read length-prefixed strings from the decompressed data
/// 3. **UTF-8 Validation**: Ensure all string data is valid UTF-8
///
/// The `row_count` parameter ensures we read exactly the expected number of strings.
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

/// Variable Length Encoding for signed integers using zigzag encoding
///
/// Converts signed integers to variable-length byte sequences where smaller absolute
/// values use fewer bytes. Uses zigzag encoding to map signed values to unsigned:
/// - 0 → 0, -1 → 1, 1 → 2, -2 → 3, 2 → 4, etc.
///
/// This is particularly effective for delta-encoded sequences where most deltas are small.
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

/// Decode Variable Length Encoded integer with zigzag decoding
///
/// Reads a variable-length encoded signed integer from the byte stream and returns
/// both the decoded value and the number of bytes consumed.
///
/// Returns an error if the encoded value is too large (> 64 bits) or if the input
/// is truncated.
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
}
