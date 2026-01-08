/*
 * Copyright (c) 2025-present Dawid Pawlik
 *
 * For educational use only by employees and students of MIMUW.
 * See LICENSE file for details.
 */

//! Example demonstrating boolean data type support in MIMDB

use mimdb::{ColumnData, Table};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("MIMDB - Boolean Data Type Example");
    println!("==================================");

    // === CREATE TABLE WITH BOOLEAN DATA ===
    println!("\n=== CREATING TABLE WITH BOOLEAN DATA ===");

    let table = create_boolean_table()?;

    println!(
        "✅ Created table with {} rows and {} columns",
        table.row_count,
        table.columns.len()
    );

    // Display table information
    table.print_metrics();

    // === SERIALIZE TO FILE ===
    println!("\n=== SERIALIZING TABLE ===");

    let filename = "boolean_example.mimdb";
    println!("Serializing table to '{}'...", filename);
    table.serialize(filename)?;

    let file_size = std::fs::metadata(filename)?.len();
    println!("✅ File saved successfully! Size: {} bytes", file_size);

    // === DESERIALIZE FROM FILE ===
    println!("\n=== DESERIALIZING TABLE ===");

    let loaded_table = Table::deserialize(filename)?;
    println!("✅ Table loaded successfully!");

    // Display the loaded table
    loaded_table.print_metrics();

    // === VERIFY DATA ===
    println!("\n=== VERIFYING BOOLEAN DATA ===");
    verify_boolean_data(&loaded_table)?;

    // Clean up
    std::fs::remove_file(filename)?;
    println!("\n✅ Example completed successfully!");

    Ok(())
}

/// Create a table with boolean data
fn create_boolean_table() -> Result<Table, Box<dyn std::error::Error>> {
    let mut table = Table::new();

    // Employee IDs
    let employee_ids = vec![1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009, 1010];
    table.add_column("employee_id".to_string(), ColumnData::Int64(employee_ids))?;

    // Employee names
    let names = vec![
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
    table.add_column("name".to_string(), ColumnData::Varchar(names))?;

    // Active employees (boolean)
    let is_active = vec![
        true, true, false, true, true, false, true, true, false, true,
    ];
    table.add_column("is_active".to_string(), ColumnData::Bool(is_active))?;

    // Manager status (boolean)
    let is_manager = vec![
        false, true, false, true, false, false, true, false, false, true,
    ];
    table.add_column("is_manager".to_string(), ColumnData::Bool(is_manager))?;

    // Remote work eligible (boolean)
    let can_work_remote = vec![
        true, true, true, false, true, false, true, true, true, false,
    ];
    table.add_column(
        "can_work_remote".to_string(),
        ColumnData::Bool(can_work_remote),
    )?;

    // Certifications completed (boolean)
    let has_certifications = vec![
        true, false, true, true, false, true, false, true, true, false,
    ];
    table.add_column(
        "has_certifications".to_string(),
        ColumnData::Bool(has_certifications),
    )?;

    Ok(table)
}

/// Verify that boolean data was correctly preserved
fn verify_boolean_data(table: &Table) -> Result<(), Box<dyn std::error::Error>> {
    println!("Checking boolean columns...");

    // Check is_active column
    if let Some(ColumnData::Bool(is_active)) = table.get_column("is_active") {
        println!(
            "  ✓ is_active: {} active employees out of {}",
            is_active.iter().filter(|&&x| x).count(),
            is_active.len()
        );
    }

    // Check is_manager column
    if let Some(ColumnData::Bool(is_manager)) = table.get_column("is_manager") {
        println!(
            "  ✓ is_manager: {} managers out of {}",
            is_manager.iter().filter(|&&x| x).count(),
            is_manager.len()
        );
    }

    // Check can_work_remote column
    if let Some(ColumnData::Bool(can_work_remote)) = table.get_column("can_work_remote") {
        println!(
            "  ✓ can_work_remote: {} employees can work remote out of {}",
            can_work_remote.iter().filter(|&&x| x).count(),
            can_work_remote.len()
        );
    }

    // Check has_certifications column
    if let Some(ColumnData::Bool(has_certifications)) = table.get_column("has_certifications") {
        println!(
            "  ✓ has_certifications: {} employees have certifications out of {}",
            has_certifications.iter().filter(|&&x| x).count(),
            has_certifications.len()
        );
    }

    println!("\n✅ All boolean data verified successfully!");
    Ok(())
}
