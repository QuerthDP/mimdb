/*
 * Copyright (c) 2025-present Dawid Pawlik
 *
 * For educational use only by employees and students of MIMUW.
 * See LICENSE file for details.
 */

//! Data analysis example demonstrating advanced analytics capabilities

use mimdb::{ColumnData, Table};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("MIMDB Library - Data Analysis Example");
    println!("====================================");

    // === CREATING SAMPLE TABLE ===
    println!("\n=== CREATING SAMPLE TABLE ===");

    let table = create_analysis_table()?;

    // Display table information
    println!(
        "✅ Created table with {} rows and {} columns",
        table.row_count,
        table.columns.len()
    );

    // Show basic metrics
    table.print_metrics();

    // === DETAILED DATA ANALYSIS ===
    println!("\n=== DETAILED DATA ANALYSIS ===");
    analyze_table_data(&table)?;

    // === CHARACTER ANALYSIS ===
    println!("\n=== CHARACTER ANALYSIS ===");
    analyze_varchar_columns(&table);

    println!("\n✅ Data analysis example completed successfully!");

    Ok(())
}

/// Create a table optimized for demonstrating analysis capabilities
fn create_analysis_table() -> Result<Table, Box<dyn std::error::Error>> {
    let mut table = Table::new();

    // Student IDs - sequential for analysis
    let student_ids = vec![
        2001, 2002, 2003, 2004, 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012,
    ];
    table.add_column("student_id".to_string(), ColumnData::Int64(student_ids))?;

    // Math scores - varied for statistical analysis
    let math_scores = vec![95, 87, 92, 88, 91, 85, 94, 89, 96, 83, 90, 97];
    table.add_column("math_score".to_string(), ColumnData::Int64(math_scores))?;

    // Science scores - different distribution
    let science_scores = vec![88, 92, 85, 90, 87, 91, 86, 93, 89, 94, 88, 85];
    table.add_column(
        "science_score".to_string(),
        ColumnData::Int64(science_scores),
    )?;

    // Study hours per week - for correlation analysis
    let study_hours = vec![15, 20, 18, 16, 22, 14, 25, 19, 24, 12, 21, 26];
    table.add_column("study_hours".to_string(), ColumnData::Int64(study_hours))?;

    // Student names - diverse for character analysis
    let student_names = vec![
        "Emma Thompson".to_string(),
        "Liam O'Connor".to_string(),
        "Sophia García".to_string(),
        "Noah Johnson".to_string(),
        "Olivia Brown".to_string(),
        "William Davis".to_string(),
        "Ava Wilson".to_string(),
        "James Miller".to_string(),
        "Isabella Moore".to_string(),
        "Benjamin Taylor".to_string(),
        "Mia Anderson".to_string(),
        "Lucas Thomas".to_string(),
    ];
    table.add_column(
        "student_name".to_string(),
        ColumnData::Varchar(student_names),
    )?;

    // Academic majors - categorical data for analysis
    let majors = vec![
        "Computer Science".to_string(),
        "Mathematics".to_string(),
        "Physics".to_string(),
        "Computer Science".to_string(),
        "Chemistry".to_string(),
        "Mathematics".to_string(),
        "Biology".to_string(),
        "Physics".to_string(),
        "Computer Science".to_string(),
        "Chemistry".to_string(),
        "Biology".to_string(),
        "Mathematics".to_string(),
    ];
    table.add_column("major".to_string(), ColumnData::Varchar(majors))?;

    // Grade levels - mixed text and numbers for character analysis
    let grade_levels = vec![
        "Freshman".to_string(),
        "Sophomore".to_string(),
        "Junior".to_string(),
        "Senior".to_string(),
        "Sophomore".to_string(),
        "Junior".to_string(),
        "Senior".to_string(),
        "Freshman".to_string(),
        "Junior".to_string(),
        "Senior".to_string(),
        "Sophomore".to_string(),
        "Freshman".to_string(),
    ];
    table.add_column("grade_level".to_string(), ColumnData::Varchar(grade_levels))?;

    Ok(table)
}

/// Analyze the data in the table to demonstrate column access and calculations
fn analyze_table_data(table: &Table) -> Result<(), Box<dyn std::error::Error>> {
    // Analyze integer columns
    let averages = table.calculate_int_averages();
    if !averages.is_empty() {
        println!("📊 Detailed Integer Column Analysis:");
        for (column_name, average) in &averages {
            if let Some(ColumnData::Int64(data)) = table.get_column(column_name) {
                let min = data.iter().min().unwrap();
                let max = data.iter().max().unwrap();
                let sum: i64 = data.iter().sum();
                let range = max - min;

                // Calculate standard deviation
                let variance: f64 = data
                    .iter()
                    .map(|&x| {
                        let diff = x as f64 - average;
                        diff * diff
                    })
                    .sum::<f64>()
                    / data.len() as f64;
                let std_dev = variance.sqrt();

                println!(
                    "  • {}: avg={:.2}, min={}, max={}, sum={}, range={}, std_dev={:.2}",
                    column_name, average, min, max, sum, range, std_dev
                );
            }
        }
    }

    // Analyze varchar columns with detailed statistics
    println!("\n📋 Varchar Column Analysis:");
    for (column_name, column_data) in &table.columns {
        if let ColumnData::Varchar(data) = column_data {
            let total_chars: usize = data.iter().map(|s| s.len()).sum();
            let avg_length = total_chars as f64 / data.len() as f64;
            let unique_values: std::collections::HashSet<_> = data.iter().collect();

            // Find longest and shortest strings
            let max_length = data.iter().map(|s| s.len()).max().unwrap_or(0);
            let min_length = data.iter().map(|s| s.len()).min().unwrap_or(0);

            println!(
                "  • {}: {} values, {} unique, avg_length={:.1}, range={}..{}",
                column_name,
                data.len(),
                unique_values.len(),
                avg_length,
                min_length,
                max_length
            );

            // Show frequency analysis for categorical data
            let mut frequency: std::collections::HashMap<&String, usize> =
                std::collections::HashMap::new();
            for value in data {
                *frequency.entry(value).or_insert(0) += 1;
            }

            if unique_values.len() <= 8 {
                // Show frequency for categorical data
                println!("    Frequency distribution:");
                let mut freq_vec: Vec<_> = frequency.iter().collect();
                freq_vec.sort_by(|a, b| b.1.cmp(a.1));
                for (value, count) in freq_vec {
                    println!("      '{}': {}", value, count);
                }
            } else {
                // Show sample values for non-categorical data
                println!(
                    "    Sample values: {:?}",
                    data.iter().take(3).collect::<Vec<_>>()
                );
            }
        }
    }

    Ok(())
}

/// Analyze character patterns in varchar columns
fn analyze_varchar_columns(table: &Table) {
    let char_counts = table.calculate_ascii_counts();

    if char_counts.is_empty() {
        println!("No varchar columns found for character analysis.");
        return;
    }

    println!("🔤 ASCII Character Analysis:");

    for (column_name, counts) in &char_counts {
        if let Some(total_ascii) = table.get_total_ascii_count(column_name) {
            println!(
                "  • Column '{}': {} total ASCII characters",
                column_name, total_ascii
            );

            // Categorize characters
            let letters: usize = counts
                .iter()
                .filter(|(ch, _)| ch.is_alphabetic())
                .map(|(_, count)| count)
                .sum();
            let digits: usize = counts
                .iter()
                .filter(|(ch, _)| ch.is_ascii_digit())
                .map(|(_, count)| count)
                .sum();
            let spaces: usize = *counts.get(&' ').unwrap_or(&0);
            let punctuation: usize = counts
                .iter()
                .filter(|(ch, _)| ch.is_ascii_punctuation())
                .map(|(_, count)| count)
                .sum();

            println!(
                "    Character breakdown: letters={}, digits={}, spaces={}, punctuation={}",
                letters, digits, spaces, punctuation
            );

            // Show top 5 most common characters
            let mut char_vec: Vec<_> = counts.iter().collect();
            char_vec.sort_by(|a, b| b.1.cmp(a.1));

            print!("    Most frequent: ");
            for (i, (ch, count)) in char_vec.iter().take(5).enumerate() {
                if i > 0 {
                    print!(", ");
                }
                let display_char = if **ch == ' ' {
                    "SPACE"
                } else {
                    &ch.to_string()
                };
                print!("'{}': {}", display_char, count);
            }
            println!();
        }
    }
}
