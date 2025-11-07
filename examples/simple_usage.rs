/*
 * Copyright (c) 2025-present Dawid Pawlik
 *
 * For educational use only by employees and students of MIMUW.
 * See LICENSE file for details.
 */

//! Comprehensive MIMDB usage example demonstrating all library features

use mimdb::{ColumnData, Table};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("MIMDB Library - Comprehensive Usage Example");
    println!("============================================");

    // === CREATING TABLE ===
    println!("\n=== CREATING TABLE ===");

    let table = create_comprehensive_table()?;

    // Display table information
    println!(
        "✅ Created table with {} rows and {} columns",
        table.row_count,
        table.columns.len()
    );

    // Show detailed metrics for the original table
    table.print_metrics();

    // === SERIALIZING TABLE ===
    println!("\n=== SERIALIZING TABLE ===");

    // Save the table to a file
    let filename = "comprehensive_example.mimdb";
    println!("Serializing table to '{}'...", filename);
    table.serialize(filename)?;

    let file_size = std::fs::metadata(filename)?.len();
    println!("✅ File saved successfully! Size: {} bytes", file_size);

    // === DESERIALIZING TABLE ===
    println!("\n=== DESERIALIZING TABLE ===");

    // Load the table back from the file
    println!("Loading table from '{}'...", filename);
    let loaded_table = Table::deserialize(filename)?;
    println!("✅ Table loaded successfully!");

    // Display metrics and analyze the loaded data
    loaded_table.print_metrics();

    // === VERIFICATION ===
    println!("\n=== VERIFICATION ===");
    verify_table_integrity(&table, &loaded_table)?;

    // Clean up
    std::fs::remove_file(filename)?;
    println!("\n✅ Example completed successfully!");

    Ok(())
}

/// Create a comprehensive table with various data types and patterns
fn create_comprehensive_table() -> Result<Table, Box<dyn std::error::Error>> {
    let mut table = Table::new();

    // Employee IDs - sequential integers
    let employee_ids = vec![1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009, 1010];
    table.add_column("employee_id".to_string(), ColumnData::Int64(employee_ids))?;

    // Test scores - varied integers
    let test_scores = vec![95, 87, 92, 88, 91, 85, 94, 89, 96, 83];
    table.add_column("test_score".to_string(), ColumnData::Int64(test_scores))?;

    // Delta compression test - values with small differences (ideal for compression)
    let sequential_data = vec![1000, 1002, 1001, 1003, 1004, 1005, 1003, 1006, 1007, 1008];
    table.add_column(
        "sequential_values".to_string(),
        ColumnData::Int64(sequential_data),
    )?;

    // Employee names - varchar data
    let employee_names = vec![
        "Alice Johnson".to_string(),
        "Bob Smith".to_string(),
        "Charlie Brown".to_string(),
        "Diana Prince".to_string(),
        "Eve Adams".to_string(),
        "Frank Miller".to_string(),
        "Grace Kelly".to_string(),
        "Henry Ford".to_string(),
        "Ivy League".to_string(),
        "Jack Wilson".to_string(),
    ];
    table.add_column("name".to_string(), ColumnData::Varchar(employee_names))?;

    // Job titles - varchar with repetitive patterns
    let job_titles = vec![
        "Software Engineer".to_string(),
        "Data Scientist".to_string(),
        "Product Manager".to_string(),
        "UX Designer".to_string(),
        "Software Engineer".to_string(), // Repeated for compression testing
        "DevOps Engineer".to_string(),
        "Data Scientist".to_string(), // Repeated
        "Backend Developer".to_string(),
        "Frontend Developer".to_string(),
        "Product Manager".to_string(), // Repeated
    ];
    table.add_column("job_title".to_string(), ColumnData::Varchar(job_titles))?;

    // Department codes - short varchar strings
    let departments = vec![
        "ENG".to_string(),
        "DATA".to_string(),
        "PROD".to_string(),
        "DESIGN".to_string(),
        "ENG".to_string(),
        "OPS".to_string(),
        "DATA".to_string(),
        "ENG".to_string(),
        "ENG".to_string(),
        "PROD".to_string(),
    ];
    table.add_column("department".to_string(), ColumnData::Varchar(departments))?;

    Ok(table)
}

/// Verify that the original and loaded tables have identical data
fn verify_table_integrity(
    original: &Table,
    loaded: &Table,
) -> Result<(), Box<dyn std::error::Error>> {
    // Check basic properties
    if original.row_count != loaded.row_count {
        return Err(format!(
            "Row count mismatch: {} vs {}",
            original.row_count, loaded.row_count
        )
        .into());
    }

    if original.columns.len() != loaded.columns.len() {
        return Err(format!(
            "Column count mismatch: {} vs {}",
            original.columns.len(),
            loaded.columns.len()
        )
        .into());
    }

    // Check each column
    for (name, original_data) in &original.columns {
        let loaded_data = loaded
            .get_column(name)
            .ok_or_else(|| format!("Column '{}' missing in loaded table", name))?;

        match (original_data, loaded_data) {
            (ColumnData::Int64(orig), ColumnData::Int64(loaded)) => {
                if orig != loaded {
                    return Err(format!("Integer column '{}' data mismatch", name).into());
                }
            }
            (ColumnData::Varchar(orig), ColumnData::Varchar(loaded)) => {
                if orig != loaded {
                    return Err(format!("Varchar column '{}' data mismatch", name).into());
                }
            }
            _ => {
                return Err(format!("Column '{}' type mismatch", name).into());
            }
        }
    }

    println!("✅ All data integrity checks passed!");
    println!("   • Row counts match: {}", original.row_count);
    println!("   • Column counts match: {}", original.columns.len());
    println!("   • All column data verified identical");

    Ok(())
}
