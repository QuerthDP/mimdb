/*
 * Copyright (c) 2025-present Dawid Pawlik
 *
 * For educational use only by employees and students of MIMUW.
 * See LICENSE file for details.
 */

//! Comprehensive tests for data integrity verification

use mimdb::{ColumnData, Table};
use std::collections::HashMap;
use tempfile::TempDir;

/// Test data integrity across save/load cycles with checksums
#[test]
fn test_data_integrity_with_checksums() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("integrity_test.mimdb");

    // Create table with known data patterns
    let mut original_table = Table::new();
    let int_data = vec![42, 100, -50, 0, 999, i64::MAX, i64::MIN];
    let str_data = vec![
        "integrity".to_string(),
        "test".to_string(),
        "data".to_string(),
        "verification".to_string(),
        "checksum".to_string(),
        "validation".to_string(),
        "security".to_string(),
    ];

    original_table
        .add_column("numbers".to_string(), ColumnData::Int64(int_data.clone()))
        .unwrap();
    original_table
        .add_column("strings".to_string(), ColumnData::Varchar(str_data.clone()))
        .unwrap();

    // Calculate checksums before saving
    let original_checksums = calculate_table_checksums(&original_table);

    // Save and load
    original_table.serialize(&file_path).unwrap();
    let loaded_table = Table::deserialize(&file_path).unwrap();

    // Calculate checksums after loading
    let loaded_checksums = calculate_table_checksums(&loaded_table);

    // Verify checksums match
    assert_eq!(
        original_checksums, loaded_checksums,
        "Checksums don't match - data integrity compromised"
    );

    // Verify actual data
    verify_column_data_integrity(&original_table, &loaded_table, "numbers");
    verify_column_data_integrity(&original_table, &loaded_table, "strings");
}

/// Test data integrity with compression patterns
#[test]
fn test_compression_data_integrity() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("compression_integrity_test.mimdb");

    let mut table = Table::new();
    let size = 100; // Use consistent size for all columns

    // Sequential data (good for delta compression)
    let sequential: Vec<i64> = (1000..(1000 + size as i64)).collect();
    table
        .add_column(
            "sequential".to_string(),
            ColumnData::Int64(sequential.clone()),
        )
        .unwrap();

    // Random data (challenging for compression)
    let base_random = [
        987654321, -123456789, 555555555, -999999999, 111111111, -777777777, 333333333, -555555555,
        888888888, -111111111,
    ];
    let random: Vec<i64> = (0..size)
        .map(|i| base_random[i % base_random.len()])
        .collect();
    table
        .add_column("random".to_string(), ColumnData::Int64(random.clone()))
        .unwrap();

    // Repetitive string data (good for compression)
    let base_repetitive = vec![
        "compress".to_string(),
        "compress".to_string(),
        "test".to_string(),
        "test".to_string(),
        "data".to_string(),
        "data".to_string(),
        "integrity".to_string(),
        "integrity".to_string(),
        "verify".to_string(),
        "verify".to_string(),
    ];
    let repetitive: Vec<String> = (0..size)
        .map(|i| base_repetitive[i % base_repetitive.len()].clone())
        .collect();
    table
        .add_column(
            "repetitive".to_string(),
            ColumnData::Varchar(repetitive.clone()),
        )
        .unwrap();

    let original_stats = calculate_detailed_stats(&table);

    table.serialize(&file_path).unwrap();
    let loaded_table = Table::deserialize(&file_path).unwrap();

    let loaded_stats = calculate_detailed_stats(&loaded_table);

    // Verify statistical properties are preserved
    assert_eq!(
        original_stats, loaded_stats,
        "Statistical properties changed during compression/decompression"
    );
}

/// Test integrity with edge cases and boundary conditions
#[test]
fn test_boundary_conditions_integrity() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("boundary_test.mimdb");

    let mut table = Table::new();

    // Boundary integer values
    let boundary_ints = vec![
        0,
        1,
        -1,
        127,
        128,
        -127,
        -128,
        255,
        256,
        -255,
        -256,
        65535,
        65536,
        -65535,
        -65536,
        i64::MAX,
        i64::MIN,
        i64::MAX - 1,
        i64::MIN + 1,
    ];
    table
        .add_column(
            "boundaries".to_string(),
            ColumnData::Int64(boundary_ints.clone()),
        )
        .unwrap();

    // Boundary string cases
    let boundary_strings = vec![
        "".to_string(),                     // Empty
        "a".to_string(),                    // Single char
        "ab".to_string(),                   // Two chars
        "🚀".to_string(),                   // Unicode
        "a".repeat(1000),                   // Long string
        "\n\r\t\0".to_string(),             // Control characters
        "\"'\\".to_string(),                // Escape characters
        " ".to_string(),                    // Whitespace
        "   ".to_string(),                  // Multiple whitespace
        "Normal text".to_string(),          // Regular string
        "123456789".to_string(),            // Numeric string
        "MiXeD cAsE".to_string(),           // Mixed case
        "UPPERCASE".to_string(),            // All caps
        "lowercase".to_string(),            // All lower
        "Special!@#$%^&*()".to_string(),    // Special characters
        "Path/To/File.ext".to_string(),     // Path-like
        "user@domain.com".to_string(),      // Email-like
        "http://example.com".to_string(),   // URL-like
        "2023-11-06T10:30:00Z".to_string(), // Timestamp-like
    ];
    table
        .add_column(
            "boundary_strings".to_string(),
            ColumnData::Varchar(boundary_strings.clone()),
        )
        .unwrap();

    table.serialize(&file_path).unwrap();
    let loaded_table = Table::deserialize(&file_path).unwrap();

    // Verify every boundary value
    if let (Some(ColumnData::Int64(original)), Some(ColumnData::Int64(loaded))) = (
        table.get_column("boundaries"),
        loaded_table.get_column("boundaries"),
    ) {
        assert_eq!(original.len(), loaded.len());
        for (i, (&orig, &load)) in original.iter().zip(loaded.iter()).enumerate() {
            assert_eq!(
                orig, load,
                "Integer boundary value mismatch at index {}: {} != {}",
                i, orig, load
            );
        }
    }

    if let (Some(ColumnData::Varchar(original)), Some(ColumnData::Varchar(loaded))) = (
        table.get_column("boundary_strings"),
        loaded_table.get_column("boundary_strings"),
    ) {
        assert_eq!(original.len(), loaded.len());
        for (i, (orig, load)) in original.iter().zip(loaded.iter()).enumerate() {
            assert_eq!(
                orig, load,
                "String boundary value mismatch at index {}: '{}' != '{}'",
                i, orig, load
            );
        }
    }
}

/// Test data integrity with incremental updates
#[test]
fn test_incremental_integrity() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("incremental_test.mimdb");

    // Start with small table
    let mut table = Table::new();
    table
        .add_column("data".to_string(), ColumnData::Int64(vec![1, 2, 3]))
        .unwrap();

    let mut expected_checksums = Vec::new();

    // Perform incremental saves and verify integrity at each step
    for iteration in 0..10 {
        // Add more data
        let new_values = vec![iteration * 10, iteration * 10 + 1, iteration * 10 + 2];

        // Create new table with additional data
        let mut new_table = Table::new();
        let mut all_data = Vec::new();

        if let Some(ColumnData::Int64(existing)) = table.get_column("data") {
            all_data.extend(existing.clone());
        }
        all_data.extend(new_values);

        new_table
            .add_column("data".to_string(), ColumnData::Int64(all_data.clone()))
            .unwrap();

        // Calculate and store checksum
        let checksum = calculate_table_checksums(&new_table);
        expected_checksums.push(checksum.clone());

        // Save and reload
        new_table.serialize(&file_path).unwrap();
        let loaded = Table::deserialize(&file_path).unwrap();

        // Verify checksum matches
        let loaded_checksum = calculate_table_checksums(&loaded);
        assert_eq!(
            checksum, loaded_checksum,
            "Checksum mismatch at iteration {}",
            iteration
        );

        // Verify data length grows as expected
        if let Some(ColumnData::Int64(loaded_data)) = loaded.get_column("data") {
            assert_eq!(
                loaded_data.len(),
                ((iteration + 1) * 3 + 3) as usize,
                "Data length mismatch at iteration {}",
                iteration
            );
        }

        table = new_table;
    }
}

/// Test concurrent data integrity (simulate concurrent access patterns)
#[test]
fn test_concurrent_access_patterns() {
    let temp_dir = TempDir::new().unwrap();

    // Simulate multiple "processes" working with the same data structure
    let original_data = create_test_table_for_concurrent_testing();
    let original_checksum = calculate_table_checksums(&original_data);

    // Simulate multiple save/load cycles from different "processes"
    for process_id in 0..5 {
        let file_path = temp_dir
            .path()
            .join(format!("concurrent_test_{}.mimdb", process_id));

        // Each "process" saves the same data
        original_data.serialize(&file_path).unwrap();

        // Each "process" loads and verifies
        let loaded = Table::deserialize(&file_path).unwrap();
        let loaded_checksum = calculate_table_checksums(&loaded);

        assert_eq!(
            original_checksum, loaded_checksum,
            "Concurrent access pattern {} failed integrity check",
            process_id
        );
    }

    // Cross-verify: load each file and ensure they're all identical
    let mut all_checksums = Vec::new();
    for process_id in 0..5 {
        let file_path = temp_dir
            .path()
            .join(format!("concurrent_test_{}.mimdb", process_id));
        let loaded = Table::deserialize(&file_path).unwrap();
        all_checksums.push(calculate_table_checksums(&loaded));
    }

    // All checksums should be identical
    for (i, checksum) in all_checksums.iter().enumerate() {
        assert_eq!(
            *checksum, original_checksum,
            "Cross-verification failed for file {}",
            i
        );
    }
}

/// Test memory vs disk data consistency
#[test]
fn test_memory_disk_consistency() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("memory_disk_test.mimdb");

    let in_memory_table = create_comprehensive_test_table();

    // Calculate comprehensive metrics while in memory
    let memory_metrics = calculate_comprehensive_metrics(&in_memory_table);

    // Save to disk
    in_memory_table.serialize(&file_path).unwrap();

    // Load from disk
    let disk_table = Table::deserialize(&file_path).unwrap();

    // Calculate same metrics from disk-loaded data
    let disk_metrics = calculate_comprehensive_metrics(&disk_table);

    // All metrics should be identical
    assert_eq!(
        memory_metrics, disk_metrics,
        "Memory and disk data metrics don't match"
    );

    // Verify table structure
    assert_eq!(in_memory_table.row_count, disk_table.row_count);
    assert_eq!(in_memory_table.columns.len(), disk_table.columns.len());

    // Verify all columns exist and have correct types
    for (name, memory_column) in &in_memory_table.columns {
        if let Some(disk_column) = disk_table.get_column(name) {
            assert_eq!(
                memory_column.column_type(),
                disk_column.column_type(),
                "Column type mismatch for column: {}",
                name
            );
            assert_eq!(
                memory_column.len(),
                disk_column.len(),
                "Column length mismatch for column: {}",
                name
            );
        } else {
            panic!("Column '{}' missing in disk-loaded table", name);
        }
    }
}

// Helper functions for integrity testing

fn calculate_table_checksums(table: &Table) -> HashMap<String, u64> {
    let mut checksums = HashMap::new();

    for (name, column) in &table.columns {
        let checksum = match column {
            ColumnData::Int64(data) => data.iter().fold(0u64, |acc, &val| {
                acc.wrapping_add((val as u64).wrapping_mul(31))
            }),
            ColumnData::Varchar(data) => data.iter().fold(0u64, |acc, s| {
                let str_hash = s
                    .bytes()
                    .fold(0u64, |acc, b| acc.wrapping_add((b as u64).wrapping_mul(37)));
                acc.wrapping_add(str_hash.wrapping_mul(31))
            }),
        };
        checksums.insert(name.clone(), checksum);
    }

    checksums
}

fn verify_column_data_integrity(original: &Table, loaded: &Table, column_name: &str) {
    match (
        original.get_column(column_name),
        loaded.get_column(column_name),
    ) {
        (Some(ColumnData::Int64(orig)), Some(ColumnData::Int64(load))) => {
            assert_eq!(orig, load, "Int64 column '{}' data mismatch", column_name);
        }
        (Some(ColumnData::Varchar(orig)), Some(ColumnData::Varchar(load))) => {
            assert_eq!(orig, load, "Varchar column '{}' data mismatch", column_name);
        }
        (Some(_), Some(_)) => {
            panic!(
                "Column '{}' type mismatch between original and loaded",
                column_name
            );
        }
        (Some(_), None) => {
            panic!("Column '{}' missing in loaded table", column_name);
        }
        (None, Some(_)) => {
            panic!(
                "Column '{}' exists in loaded but not original table",
                column_name
            );
        }
        (None, None) => {
            panic!("Column '{}' doesn't exist in either table", column_name);
        }
    }
}

fn calculate_detailed_stats(table: &Table) -> HashMap<String, String> {
    let mut stats = HashMap::new();

    stats.insert("row_count".to_string(), table.row_count.to_string());
    stats.insert("column_count".to_string(), table.columns.len().to_string());

    for (name, column) in &table.columns {
        match column {
            ColumnData::Int64(data) => {
                let sum: i64 = data.iter().sum();
                let min = data.iter().min().unwrap_or(&0);
                let max = data.iter().max().unwrap_or(&0);
                stats.insert(format!("{}_sum", name), sum.to_string());
                stats.insert(format!("{}_min", name), min.to_string());
                stats.insert(format!("{}_max", name), max.to_string());
                stats.insert(format!("{}_len", name), data.len().to_string());
            }
            ColumnData::Varchar(data) => {
                let total_len: usize = data.iter().map(|s| s.len()).sum();
                let min_len = data.iter().map(|s| s.len()).min().unwrap_or(0);
                let max_len = data.iter().map(|s| s.len()).max().unwrap_or(0);
                stats.insert(format!("{}_total_chars", name), total_len.to_string());
                stats.insert(format!("{}_min_len", name), min_len.to_string());
                stats.insert(format!("{}_max_len", name), max_len.to_string());
                stats.insert(format!("{}_len", name), data.len().to_string());
            }
        }
    }

    stats
}

fn create_test_table_for_concurrent_testing() -> Table {
    let mut table = Table::new();

    let ids: Vec<i64> = (1..=100).collect();
    let names: Vec<String> = (1..=100).map(|i| format!("Item_{}", i)).collect();

    table
        .add_column("ids".to_string(), ColumnData::Int64(ids))
        .unwrap();
    table
        .add_column("names".to_string(), ColumnData::Varchar(names))
        .unwrap();

    table
}

fn create_comprehensive_test_table() -> Table {
    let mut table = Table::new();

    // Various data patterns
    table
        .add_column(
            "sequential".to_string(),
            ColumnData::Int64((0..50).collect()),
        )
        .unwrap();

    table
        .add_column(
            "random".to_string(),
            ColumnData::Int64(vec![
                987, -123, 456, -789, 321, -654, 147, -258, 369, -741, 852, -963, 159, -357, 486,
                -951, 753, -824, 617, -395, 428, -571, 639, -283, 746, -192, 835, -467, 529, -618,
                374, -825, 591, -736, 482, -159, 673, -948, 207, -564, 819, -302, 685, -437, 508,
                -721, 346, -692, 175, -583,
            ]),
        )
        .unwrap();

    let strings: Vec<String> = (0..50)
        .map(|i| match i % 5 {
            0 => format!("Type_A_{}", i),
            1 => format!("Type_B_{}", i),
            2 => format!("Type_C_{}", i),
            3 => format!("Type_D_{}", i),
            _ => format!("Type_E_{}", i),
        })
        .collect();

    table
        .add_column("categories".to_string(), ColumnData::Varchar(strings))
        .unwrap();

    table
}

fn calculate_comprehensive_metrics(table: &Table) -> HashMap<String, f64> {
    let mut metrics = HashMap::new();

    // Basic metrics
    metrics.insert("row_count".to_string(), table.row_count as f64);
    metrics.insert("column_count".to_string(), table.columns.len() as f64);

    // Per-column metrics
    for (name, column) in &table.columns {
        match column {
            ColumnData::Int64(data) => {
                if !data.is_empty() {
                    let sum: i64 = data.iter().sum();
                    let mean = sum as f64 / data.len() as f64;
                    let variance = data.iter().map(|&x| (x as f64 - mean).powi(2)).sum::<f64>()
                        / data.len() as f64;

                    metrics.insert(format!("{}_mean", name), mean);
                    metrics.insert(format!("{}_variance", name), variance);
                    metrics.insert(format!("{}_min", name), *data.iter().min().unwrap() as f64);
                    metrics.insert(format!("{}_max", name), *data.iter().max().unwrap() as f64);
                }
            }
            ColumnData::Varchar(data) => {
                let total_len: usize = data.iter().map(|s| s.len()).sum();
                let mean_len = total_len as f64 / data.len() as f64;

                metrics.insert(format!("{}_mean_length", name), mean_len);
                metrics.insert(format!("{}_total_length", name), total_len as f64);

                if !data.is_empty() {
                    let min_len = data.iter().map(|s| s.len()).min().unwrap();
                    let max_len = data.iter().map(|s| s.len()).max().unwrap();
                    metrics.insert(format!("{}_min_length", name), min_len as f64);
                    metrics.insert(format!("{}_max_length", name), max_len as f64);
                }
            }
        }
    }

    metrics
}
