/*
 * Copyright (c) 2025-present Dawid Pawlik
 *
 * For educational use only by employees and students of MIMUW.
 * See LICENSE file for details.
 */

//! Main test runner and utilities for MIMDB comprehensive test suite
//!
//! This module provides utilities and integration tests that combine
//! all the different test categories for comprehensive validation.

use mimdb::{ColumnData, Table};
use std::fs;
use tempfile::TempDir;

/// Comprehensive integration test that exercises all major functionality
#[test]
fn test_comprehensive_integration() {
    let temp_dir = TempDir::new().unwrap();

    println!("Running comprehensive integration test...");

    // Test 1: Create complex table with multiple data types
    let complex_table = create_complex_test_table();
    let original_metrics = calculate_table_metrics(&complex_table);

    // Test 2: Serialize and deserialize
    let file_path = temp_dir.path().join("complex_test.mimdb");
    complex_table.save_to_file(&file_path).unwrap();
    assert!(file_path.exists(), "File should be created");

    let loaded_table = Table::load_from_file(&file_path).unwrap();
    let loaded_metrics = calculate_table_metrics(&loaded_table);

    // Test 3: Verify data integrity
    assert_eq!(
        original_metrics, loaded_metrics,
        "Metrics should be identical"
    );
    verify_complex_table_data(&loaded_table);

    // Test 4: Multiple round trips
    for i in 0..3 {
        let round_trip_path = temp_dir.path().join(format!("round_trip_{}.mimdb", i));
        loaded_table.save_to_file(&round_trip_path).unwrap();

        let round_trip_table = Table::load_from_file(&round_trip_path).unwrap();
        let round_trip_metrics = calculate_table_metrics(&round_trip_table);

        assert_eq!(
            original_metrics, round_trip_metrics,
            "Round trip {} should preserve metrics",
            i
        );
    }

    println!("Comprehensive integration test passed!");
}

/// Test edge cases and boundary conditions together
#[test]
fn test_edge_cases_integration() {
    let temp_dir = TempDir::new().unwrap();

    // Test with empty table
    let empty_table = Table::new();
    let empty_path = temp_dir.path().join("empty.mimdb");
    empty_table.save_to_file(&empty_path).unwrap();
    let empty_loaded = Table::load_from_file(&empty_path).unwrap();
    assert_eq!(empty_loaded.row_count, 0);
    assert_eq!(empty_loaded.columns.len(), 0);

    // Test with single column single row
    let mut minimal_table = Table::new();
    minimal_table
        .add_column("single".to_string(), ColumnData::Int64(vec![42]))
        .unwrap();
    let minimal_path = temp_dir.path().join("minimal.mimdb");
    minimal_table.save_to_file(&minimal_path).unwrap();
    let minimal_loaded = Table::load_from_file(&minimal_path).unwrap();
    assert_eq!(minimal_loaded.row_count, 1);

    // Test with extreme values
    let mut extreme_table = Table::new();
    extreme_table
        .add_column(
            "extremes".to_string(),
            ColumnData::Int64(vec![i64::MIN, i64::MAX, 0]),
        )
        .unwrap();
    extreme_table
        .add_column(
            "special_strings".to_string(),
            ColumnData::Varchar(vec![
                "".to_string(),
                "🌟".to_string(),
                "Line1\nLine2\tTab".to_string(),
            ]),
        )
        .unwrap();

    let extreme_path = temp_dir.path().join("extreme.mimdb");
    extreme_table.save_to_file(&extreme_path).unwrap();
    let extreme_loaded = Table::load_from_file(&extreme_path).unwrap();

    if let Some(ColumnData::Int64(data)) = extreme_loaded.get_column("extremes") {
        assert_eq!(data, &vec![i64::MIN, i64::MAX, 0]);
    }
}

/// Performance and scalability integration test
#[test]
fn test_performance_integration() {
    let temp_dir = TempDir::new().unwrap();

    // Create progressively larger tables to test scalability
    let sizes = vec![100usize, 1000, 10000];

    for size in sizes {
        println!("Testing performance with {} rows", size);

        let mut large_table = Table::new();

        // Sequential data (good for compression)
        let sequential: Vec<i64> = (0..size as i64).collect();
        large_table
            .add_column(
                "sequential".to_string(),
                ColumnData::Int64(sequential.clone()),
            )
            .unwrap();

        // Random-ish data (challenging for compression)
        let random_data: Vec<i64> = (0..size).map(|i| ((i * 17 + 42) % 1000) as i64).collect();
        large_table
            .add_column("random".to_string(), ColumnData::Int64(random_data.clone()))
            .unwrap();

        // String data with patterns
        let string_data: Vec<String> = (0..size)
            .map(|i| format!("Item_{:06}_{}", i, i % 10))
            .collect();
        large_table
            .add_column(
                "strings".to_string(),
                ColumnData::Varchar(string_data.clone()),
            )
            .unwrap();

        // Time the serialization
        let start = std::time::Instant::now();
        let file_path = temp_dir.path().join(format!("perf_test_{}.mimdb", size));
        large_table.save_to_file(&file_path).unwrap();
        let save_duration = start.elapsed();

        // Time the deserialization
        let start = std::time::Instant::now();
        let loaded_table = Table::load_from_file(&file_path).unwrap();
        let load_duration = start.elapsed();

        // Verify correctness
        assert_eq!(loaded_table.row_count, size);
        assert_eq!(loaded_table.columns.len(), 3);

        // Check compression effectiveness
        let file_size = fs::metadata(&file_path).unwrap().len();
        let uncompressed_estimate = size * (8 + 8 + 20); // rough estimate for uncompressed size
        let compression_ratio = file_size as f64 / uncompressed_estimate as f64;

        println!(
            "Size: {}, Save: {:?}, Load: {:?}, File size: {} bytes, Compression ratio: {:.2}",
            size, save_duration, load_duration, file_size, compression_ratio
        );

        // Verify some data integrity
        if let Some(ColumnData::Int64(loaded_seq)) = loaded_table.get_column("sequential") {
            assert_eq!(loaded_seq[0], 0);
            assert_eq!(loaded_seq[size - 1], (size - 1) as i64);
        }
    }
}

/// Test cross-compatibility between different table configurations
#[test]
fn test_cross_compatibility() {
    let temp_dir = TempDir::new().unwrap();

    // Create tables with different characteristics
    let test_tables = vec![
        ("int_only", create_int_only_table()),
        ("string_only", create_string_only_table()),
        ("mixed_small", create_mixed_small_table()),
        ("mixed_large", create_mixed_large_table()),
    ];

    // Save all tables
    let mut saved_files = Vec::new();
    for (name, table) in &test_tables {
        let path = temp_dir.path().join(format!("{}.mimdb", name));
        table.save_to_file(&path).unwrap();
        saved_files.push((name, path, table));
    }

    // Cross-verify: each file should load correctly regardless of order
    for (name, path, original_table) in &saved_files {
        let loaded = Table::load_from_file(path).unwrap();

        // Basic structure verification
        assert_eq!(
            loaded.row_count, original_table.row_count,
            "Row count mismatch for {}",
            name
        );
        assert_eq!(
            loaded.columns.len(),
            original_table.columns.len(),
            "Column count mismatch for {}",
            name
        );

        // Data verification
        for (col_name, original_data) in &original_table.columns {
            if let Some(loaded_data) = loaded.get_column(col_name) {
                match (original_data, loaded_data) {
                    (ColumnData::Int64(orig), ColumnData::Int64(load)) => {
                        assert_eq!(
                            orig, load,
                            "Int64 data mismatch in {} column {}",
                            name, col_name
                        );
                    }
                    (ColumnData::Varchar(orig), ColumnData::Varchar(load)) => {
                        assert_eq!(
                            orig, load,
                            "Varchar data mismatch in {} column {}",
                            name, col_name
                        );
                    }
                    _ => panic!("Type mismatch in {} column {}", name, col_name),
                }
            } else {
                panic!("Missing column {} in loaded table {}", col_name, name);
            }
        }
    }
}

/// Stress test with unusual but valid data patterns
#[test]
fn test_stress_patterns() {
    let temp_dir = TempDir::new().unwrap();

    // Test 1: All zeros
    let mut zeros_table = Table::new();
    zeros_table
        .add_column("zeros".to_string(), ColumnData::Int64(vec![0; 1000]))
        .unwrap();
    zeros_table
        .add_column(
            "empty_strings".to_string(),
            ColumnData::Varchar(vec!["".to_string(); 1000]),
        )
        .unwrap();

    let zeros_path = temp_dir.path().join("zeros.mimdb");
    zeros_table.save_to_file(&zeros_path).unwrap();
    let zeros_loaded = Table::load_from_file(&zeros_path).unwrap();
    assert_eq!(zeros_loaded.row_count, 1000);

    // Test 2: All same non-zero values
    let mut same_table = Table::new();
    same_table
        .add_column("same_ints".to_string(), ColumnData::Int64(vec![12345; 500]))
        .unwrap();
    same_table
        .add_column(
            "same_strings".to_string(),
            ColumnData::Varchar(vec!["identical".to_string(); 500]),
        )
        .unwrap();

    let same_path = temp_dir.path().join("same.mimdb");
    same_table.save_to_file(&same_path).unwrap();
    let same_loaded = Table::load_from_file(&same_path).unwrap();
    assert_eq!(same_loaded.row_count, 500);

    // Test 3: Alternating pattern
    let mut alternating_table = Table::new();
    let alternating_ints: Vec<i64> = (0..1000).map(|i| if i % 2 == 0 { 1 } else { -1 }).collect();
    let alternating_strings: Vec<String> = (0..1000)
        .map(|i| {
            if i % 2 == 0 {
                "A".to_string()
            } else {
                "B".to_string()
            }
        })
        .collect();

    alternating_table
        .add_column(
            "alternating_ints".to_string(),
            ColumnData::Int64(alternating_ints.clone()),
        )
        .unwrap();
    alternating_table
        .add_column(
            "alternating_strings".to_string(),
            ColumnData::Varchar(alternating_strings.clone()),
        )
        .unwrap();

    let alternating_path = temp_dir.path().join("alternating.mimdb");
    alternating_table.save_to_file(&alternating_path).unwrap();
    let alternating_loaded = Table::load_from_file(&alternating_path).unwrap();

    // Verify the pattern is preserved
    if let Some(ColumnData::Int64(loaded_ints)) = alternating_loaded.get_column("alternating_ints")
    {
        for (i, &value) in loaded_ints.iter().enumerate() {
            let expected = if i % 2 == 0 { 1 } else { -1 };
            assert_eq!(value, expected, "Alternating pattern broken at index {}", i);
        }
    }
}

// Helper functions for comprehensive testing

fn create_complex_test_table() -> Table {
    let mut table = Table::new();
    let size = 50usize;

    // Multiple integer columns with different patterns
    table
        .add_column(
            "sequential".to_string(),
            ColumnData::Int64((1..=size as i64).collect()),
        )
        .unwrap();

    // Repeat powers of two pattern to match size
    let mut powers_of_two = Vec::new();
    for i in 0..size {
        powers_of_two.push(1i64 << (i % 20)); // Use modulo to avoid overflow
    }
    table
        .add_column(
            "powers_of_two".to_string(),
            ColumnData::Int64(powers_of_two),
        )
        .unwrap();

    // Extend fibonacci to match size
    let mut fibonacci = generate_fibonacci(50);
    fibonacci.resize(size, 0); // Pad with zeros if needed
    table
        .add_column("fibonacci".to_string(), ColumnData::Int64(fibonacci))
        .unwrap();

    // Multiple string columns with different characteristics
    let names = (1..=50).map(|i| format!("Person_{:03}", i)).collect();
    table
        .add_column("names".to_string(), ColumnData::Varchar(names))
        .unwrap();

    let categories = ["A", "B", "C", "D", "E"]
        .iter()
        .cycle()
        .take(50)
        .map(|s| s.to_string())
        .collect();
    table
        .add_column("categories".to_string(), ColumnData::Varchar(categories))
        .unwrap();

    let descriptions: Vec<String> = (1..=50).map(|i| {
        format!("This is a longer description for item number {} with variable length content and details", i)
    }).collect();
    table
        .add_column(
            "descriptions".to_string(),
            ColumnData::Varchar(descriptions),
        )
        .unwrap();

    table
}

fn generate_fibonacci(n: usize) -> Vec<i64> {
    let mut fib = vec![0, 1];
    for i in 2..n {
        let next = fib[i - 1] + fib[i - 2];
        fib.push(next);
    }
    fib
}

fn calculate_table_metrics(table: &Table) -> TableMetrics {
    let mut int_sums = std::collections::HashMap::new();
    let mut string_lengths = std::collections::HashMap::new();

    for (name, column) in &table.columns {
        match column {
            ColumnData::Int64(data) => {
                let sum: i64 = data.iter().sum();
                int_sums.insert(name.clone(), sum);
            }
            ColumnData::Varchar(data) => {
                let total_len: usize = data.iter().map(|s| s.len()).sum();
                string_lengths.insert(name.clone(), total_len);
            }
        }
    }

    TableMetrics {
        row_count: table.row_count,
        column_count: table.columns.len(),
        int_sums,
        string_lengths,
    }
}

fn verify_complex_table_data(table: &Table) {
    // Verify sequential data
    if let Some(ColumnData::Int64(seq)) = table.get_column("sequential") {
        for (i, &value) in seq.iter().enumerate() {
            assert_eq!(
                value,
                (i + 1) as i64,
                "Sequential data mismatch at index {}",
                i
            );
        }
    }

    // Verify powers of two (with modulo pattern)
    if let Some(ColumnData::Int64(powers)) = table.get_column("powers_of_two") {
        for (i, &value) in powers.iter().enumerate() {
            assert_eq!(
                value,
                1i64 << (i % 20),
                "Powers of two mismatch at index {}",
                i
            );
        }
    }

    // Verify fibonacci
    if let Some(ColumnData::Int64(fib)) = table.get_column("fibonacci") {
        assert_eq!(fib[0], 0);
        assert_eq!(fib[1], 1);
        for i in 2..fib.len() {
            assert_eq!(
                fib[i],
                fib[i - 1] + fib[i - 2],
                "Fibonacci sequence broken at index {}",
                i
            );
        }
    }
}

fn create_int_only_table() -> Table {
    let mut table = Table::new();
    table
        .add_column("col1".to_string(), ColumnData::Int64(vec![1, 2, 3, 4, 5]))
        .unwrap();
    table
        .add_column(
            "col2".to_string(),
            ColumnData::Int64(vec![10, 20, 30, 40, 50]),
        )
        .unwrap();
    table
}

fn create_string_only_table() -> Table {
    let mut table = Table::new();
    table
        .add_column(
            "col1".to_string(),
            ColumnData::Varchar(vec![
                "apple".to_string(),
                "banana".to_string(),
                "cherry".to_string(),
            ]),
        )
        .unwrap();
    table
        .add_column(
            "col2".to_string(),
            ColumnData::Varchar(vec![
                "red".to_string(),
                "yellow".to_string(),
                "red".to_string(),
            ]),
        )
        .unwrap();
    table
}

fn create_mixed_small_table() -> Table {
    let mut table = Table::new();
    table
        .add_column("numbers".to_string(), ColumnData::Int64(vec![1, 2, 3]))
        .unwrap();
    table
        .add_column(
            "words".to_string(),
            ColumnData::Varchar(vec![
                "one".to_string(),
                "two".to_string(),
                "three".to_string(),
            ]),
        )
        .unwrap();
    table
}

fn create_mixed_large_table() -> Table {
    let mut table = Table::new();
    let size = 1_000_000;

    let numbers: Vec<i64> = (0..size).collect();
    let words: Vec<String> = (0..size).map(|i| format!("word_{}", i)).collect();

    table
        .add_column("numbers".to_string(), ColumnData::Int64(numbers))
        .unwrap();
    table
        .add_column("words".to_string(), ColumnData::Varchar(words))
        .unwrap();
    table
}

#[derive(Debug, PartialEq)]
struct TableMetrics {
    row_count: usize,
    column_count: usize,
    int_sums: std::collections::HashMap<String, i64>,
    string_lengths: std::collections::HashMap<String, usize>,
}
