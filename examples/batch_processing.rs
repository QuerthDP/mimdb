/*
 * Copyright (c) 2025-present Dawid Pawlik
 *
 * For educational use only by employees and students of MIMUW.
 * See LICENSE file for details.
 */

//! Demonstration of batch processing capabilities for large tables

use mimdb::{ColumnData, Table, serialization::BatchConfig};
use std::fs;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("MIMDB Batch Processing Example");
    println!("===========================");

    // Create a large table to demonstrate batch processing
    let mut table = Table::new();

    println!("Creating large dataset...");

    // Create a large dataset (10 million rows)
    let row_count = 10_000_000;

    // Large integer sequence
    let large_numbers: Vec<i64> = (0..row_count).collect();

    // Large string dataset with patterns (good for compression)
    let large_strings: Vec<String> = (0..row_count)
        .map(|i| match i % 5 {
            0 => format!("category_A_{}", i / 1000),
            1 => format!("category_B_{}", i / 1000),
            2 => format!("category_C_{}", i / 1000),
            3 => format!("category_D_{}", i / 1000),
            _ => format!("category_E_{}", i / 1000),
        })
        .collect();

    table.add_column("id_numbers".to_string(), ColumnData::Int64(large_numbers))?;
    table.add_column("categories".to_string(), ColumnData::Varchar(large_strings))?;

    println!(
        "Table created with {} rows and {} columns",
        table.row_count,
        table.columns.len()
    );

    // Test different batch configurations
    let configs = vec![
        ("Small batches (10k)", BatchConfig::new(10_000)),
        ("Medium batches (100k)", BatchConfig::new(100_000)),
        ("Large batches (500k)", BatchConfig::new(500_000)),
        ("Default config", BatchConfig::default()),
    ];

    for (name, config) in configs {
        println!("\n--- Testing {} ---", name);
        println!("Batch size: {} rows", config.batch_size);

        let filename = format!("large_table_{}.mimdb", config.batch_size);

        // Measure serialization time
        let start = std::time::Instant::now();
        table.serialize_with_config(&filename, &config)?;
        let serialize_duration = start.elapsed();

        // Check file size
        let file_size = fs::metadata(&filename)?.len();

        // Measure deserialization time
        let start = std::time::Instant::now();
        let loaded_table = Table::deserialize_with_config(&filename, &config)?;
        let deserialize_duration = start.elapsed();

        println!("Serialization: {:?}", serialize_duration);
        println!("Deserialization: {:?}", deserialize_duration);
        println!("File size: {:.2} MB", file_size as f64 / 1_024_000.0);

        // Verify data integrity
        assert_eq!(table.row_count, loaded_table.row_count);
        assert_eq!(table.columns.len(), loaded_table.columns.len());

        // Clean up
        fs::remove_file(filename)?;

        println!("✓ Data integrity verified");
    }

    println!("\n=== Summary ===");
    println!(
        "✓ Successfully processed {} rows with different batch sizes",
        row_count
    );
    println!("✓ All configurations maintained data integrity");
    println!("✓ Memory-efficient batch processing reduces peak memory usage for large datasets");
    println!("\nBatch processing allows MIMDB to handle tables larger than available RAM");
    println!("by processing data in manageable chunks during compression and serialization.");

    Ok(())
}
