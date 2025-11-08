/*
 * Copyright (c) 2025-present Dawid Pawlik
 *
 * For educational use only by employees and students of MIMUW.
 * See LICENSE file for details.
 */

//! # MIMDB File Loader
//!
//! A command-line utility that loads and analyzes MIMDB files.
//! Takes a file path as argument, deserializes the table, and prints metrics.

use mimdb::Table;
use std::env;
use std::process;

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() != 2 {
        eprintln!("Usage: {} <file.mimdb>", args[0]);
        process::exit(1);
    }

    let file_path = &args[1];

    println!("Loading MIMDB file: {}", file_path);

    // Deserialize the table
    let table = match Table::deserialize(file_path) {
        Ok(table) => {
            println!("✓ Successfully loaded table from {}", file_path);
            table
        }
        Err(e) => {
            eprintln!("✗ Error loading file '{}': {}", file_path, e);
            process::exit(1);
        }
    };

    // Print detailed file information
    println!("\n=== FILE INFORMATION ===");
    println!("File: {}", file_path);

    // Print table metrics
    table.print_metrics();

    // Print column details
    println!("\n=== COLUMN DETAILS ===");
    for (name, column_data) in &table.columns {
        let column_type = match column_data {
            mimdb::ColumnData::Int64(_) => "Int64",
            mimdb::ColumnData::Varchar(_) => "Varchar",
        };
        let row_count = column_data.len();
        println!("  {} ({}): {} rows", name, column_type, row_count);
    }

    println!("\n✓ Analysis complete");
}
