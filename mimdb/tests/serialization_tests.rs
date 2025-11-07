/*
 * Copyright (c) 2025-present Dawid Pawlik
 *
 * For educational use only by employees and students of MIMUW.
 * See LICENSE file for details.
 */

//! Comprehensive tests for serialization and deserialization functionality

use mimdb::{ColumnData, Table};
use std::fs;
use tempfile::TempDir;

/// Test basic serialization and deserialization of simple table
#[test]
fn test_basic_serialization() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("basic_test.mimdb");

    // Create a simple table
    let mut table = Table::new();
    table
        .add_column("ids".to_string(), ColumnData::Int64(vec![1, 2, 3, 4, 5]))
        .unwrap();
    table
        .add_column(
            "names".to_string(),
            ColumnData::Varchar(vec![
                "Alice".to_string(),
                "Bob".to_string(),
                "Charlie".to_string(),
                "Diana".to_string(),
                "Eve".to_string(),
            ]),
        )
        .unwrap();

    // Serialize
    table.save_to_file(&file_path).unwrap();
    assert!(file_path.exists());

    // Deserialize
    let loaded_table = Table::load_from_file(&file_path).unwrap();

    // Verify structure
    assert_eq!(table.row_count, loaded_table.row_count);
    assert_eq!(table.columns.len(), loaded_table.columns.len());

    // Verify data
    if let (Some(ColumnData::Int64(original)), Some(ColumnData::Int64(loaded))) =
        (table.get_column("ids"), loaded_table.get_column("ids"))
    {
        assert_eq!(original, loaded);
    } else {
        panic!("Int64 column mismatch");
    }

    if let (Some(ColumnData::Varchar(original)), Some(ColumnData::Varchar(loaded))) =
        (table.get_column("names"), loaded_table.get_column("names"))
    {
        assert_eq!(original, loaded);
    } else {
        panic!("Varchar column mismatch");
    }
}

/// Test serialization with empty table
#[test]
fn test_empty_table_serialization() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("empty_test.mimdb");

    let table = Table::new();

    // Should be able to serialize empty table
    table.save_to_file(&file_path).unwrap();
    assert!(file_path.exists());

    // Should be able to deserialize empty table
    let loaded_table = Table::load_from_file(&file_path).unwrap();
    assert_eq!(loaded_table.row_count, 0);
    assert_eq!(loaded_table.columns.len(), 0);
}

/// Test serialization with single row
#[test]
fn test_single_row_serialization() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("single_row_test.mimdb");

    let mut table = Table::new();
    table
        .add_column("id".to_string(), ColumnData::Int64(vec![42]))
        .unwrap();
    table
        .add_column(
            "name".to_string(),
            ColumnData::Varchar(vec!["Test".to_string()]),
        )
        .unwrap();

    table.save_to_file(&file_path).unwrap();
    let loaded_table = Table::load_from_file(&file_path).unwrap();

    assert_eq!(loaded_table.row_count, 1);
    assert_eq!(loaded_table.columns.len(), 2);

    if let Some(ColumnData::Int64(data)) = loaded_table.get_column("id") {
        assert_eq!(data, &vec![42]);
    } else {
        panic!("Failed to load single int64 value");
    }
}

/// Test serialization with large dataset
#[test]
fn test_large_dataset_serialization() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("large_test.mimdb");

    let size = 10000usize;
    let mut table = Table::new();

    // Generate large sequential data
    let ids: Vec<i64> = (0..size as i64).collect();
    table
        .add_column("ids".to_string(), ColumnData::Int64(ids.clone()))
        .unwrap();

    // Generate large string data
    let names: Vec<String> = (0..size).map(|i| format!("User_{:05}", i)).collect();
    table
        .add_column("names".to_string(), ColumnData::Varchar(names.clone()))
        .unwrap();

    table.save_to_file(&file_path).unwrap();
    let loaded_table = Table::load_from_file(&file_path).unwrap();

    assert_eq!(loaded_table.row_count, size);

    // Verify some data points
    if let Some(ColumnData::Int64(loaded_ids)) = loaded_table.get_column("ids") {
        assert_eq!(loaded_ids[0], 0);
        assert_eq!(loaded_ids[size - 1], (size - 1) as i64);
        assert_eq!(loaded_ids.len(), size);
    } else {
        panic!("Failed to load large int64 dataset");
    }

    if let Some(ColumnData::Varchar(loaded_names)) = loaded_table.get_column("names") {
        assert_eq!(loaded_names[0], "User_00000");
        assert_eq!(loaded_names[size - 1], format!("User_{:05}", size - 1));
        assert_eq!(loaded_names.len(), size);
    } else {
        panic!("Failed to load large varchar dataset");
    }
}

/// Test serialization with special characters in strings
#[test]
fn test_special_characters_serialization() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("special_chars_test.mimdb");

    let mut table = Table::new();

    let special_strings = vec![
        "".to_string(), // Empty string
        "Hello, World! 🌍".to_string(), // Unicode
        "Line1\nLine2\nLine3".to_string(), // Newlines
        "Tabs\t\tHere".to_string(), // Tabs
        "Quote\"Inside\"String".to_string(), // Quotes
        "Null\0Character".to_string(), // Null character
        "Very long string that should test the compression algorithm's ability to handle longer text data efficiently".to_string(),
    ];

    table
        .add_column(
            "special".to_string(),
            ColumnData::Varchar(special_strings.clone()),
        )
        .unwrap();

    table.save_to_file(&file_path).unwrap();
    let loaded_table = Table::load_from_file(&file_path).unwrap();

    if let Some(ColumnData::Varchar(loaded_strings)) = loaded_table.get_column("special") {
        assert_eq!(*loaded_strings, special_strings);
    } else {
        panic!("Failed to load special character strings");
    }
}

/// Test serialization with extreme integer values
#[test]
fn test_extreme_values_serialization() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("extreme_values_test.mimdb");

    let mut table = Table::new();

    let extreme_values = vec![
        i64::MIN,
        i64::MIN + 1,
        -1000000000,
        -1,
        0,
        1,
        1000000000,
        i64::MAX - 1,
        i64::MAX,
    ];

    table
        .add_column(
            "extremes".to_string(),
            ColumnData::Int64(extreme_values.clone()),
        )
        .unwrap();

    table.save_to_file(&file_path).unwrap();
    let loaded_table = Table::load_from_file(&file_path).unwrap();

    if let Some(ColumnData::Int64(loaded_values)) = loaded_table.get_column("extremes") {
        assert_eq!(*loaded_values, extreme_values);
    } else {
        panic!("Failed to load extreme integer values");
    }
}

/// Test multiple serialization/deserialization cycles
#[test]
fn test_multiple_cycles() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("cycles_test.mimdb");

    let mut table = Table::new();
    table
        .add_column(
            "data".to_string(),
            ColumnData::Int64(vec![10, 20, 30, 40, 50]),
        )
        .unwrap();

    // Perform multiple save/load cycles
    for cycle in 0..5 {
        table.save_to_file(&file_path).unwrap();
        table = Table::load_from_file(&file_path).unwrap();

        // Verify data integrity after each cycle
        if let Some(ColumnData::Int64(data)) = table.get_column("data") {
            assert_eq!(data, &vec![10, 20, 30, 40, 50], "Cycle {} failed", cycle);
        } else {
            panic!("Cycle {} failed: no data column", cycle);
        }
    }
}

/// Test serialization with mixed column types
#[test]
fn test_mixed_column_types() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("mixed_types_test.mimdb");

    let mut table = Table::new();

    // Add multiple columns of different types
    table
        .add_column("int_col1".to_string(), ColumnData::Int64(vec![1, 2, 3]))
        .unwrap();
    table
        .add_column(
            "str_col1".to_string(),
            ColumnData::Varchar(vec!["a".to_string(), "b".to_string(), "c".to_string()]),
        )
        .unwrap();
    table
        .add_column(
            "int_col2".to_string(),
            ColumnData::Int64(vec![100, 200, 300]),
        )
        .unwrap();
    table
        .add_column(
            "str_col2".to_string(),
            ColumnData::Varchar(vec!["x".to_string(), "y".to_string(), "z".to_string()]),
        )
        .unwrap();

    table.save_to_file(&file_path).unwrap();
    let loaded_table = Table::load_from_file(&file_path).unwrap();

    assert_eq!(loaded_table.row_count, 3);
    assert_eq!(loaded_table.columns.len(), 4);

    // Verify each column
    if let Some(ColumnData::Int64(data)) = loaded_table.get_column("int_col1") {
        assert_eq!(data, &vec![1, 2, 3]);
    } else {
        panic!("int_col1 not found or wrong type");
    }

    if let Some(ColumnData::Varchar(data)) = loaded_table.get_column("str_col1") {
        assert_eq!(
            data,
            &vec!["a".to_string(), "b".to_string(), "c".to_string()]
        );
    } else {
        panic!("str_col1 not found or wrong type");
    }
}

/// Test file format validation
#[test]
fn test_invalid_file_format() {
    let temp_dir = TempDir::new().unwrap();
    let invalid_file = temp_dir.path().join("invalid.mimdb");

    // Write invalid content
    fs::write(&invalid_file, b"This is not a valid MIMDB file").unwrap();

    // Should fail to load
    assert!(Table::load_from_file(&invalid_file).is_err());
}

/// Test corrupted file handling
#[test]
fn test_corrupted_file_handling() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("corrupted_test.mimdb");

    // Create a valid file first
    let mut table = Table::new();
    table
        .add_column("test".to_string(), ColumnData::Int64(vec![1, 2, 3]))
        .unwrap();
    table.save_to_file(&file_path).unwrap();

    // Corrupt the file by truncating it
    let original_data = fs::read(&file_path).unwrap();
    let corrupted_data = &original_data[..original_data.len() / 2];
    fs::write(&file_path, corrupted_data).unwrap();

    // Should fail to load corrupted file
    assert!(Table::load_from_file(&file_path).is_err());
}
