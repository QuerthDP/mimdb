/*
 * Copyright (c) 2025-present Dawid Pawlik
 *
 * For educational use only by employees and students of MIMUW.
 * See LICENSE file for details.
 */

//! Utility to generate example data files for testing

use mimdb::{ColumnData, Table};
use std::fs;
use std::path::Path;

/// Generate all example data files
pub fn generate_all_example_files() -> Result<(), Box<dyn std::error::Error>> {
    let project_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let data_dir = project_root.join("../examples/data");

    // Ensure directory exists
    fs::create_dir_all(&data_dir)?;

    let data_dir_str = data_dir.to_str().unwrap();

    generate_simple_example(data_dir_str)?;
    generate_employee_example(data_dir_str)?;
    generate_sales_example(data_dir_str)?;
    generate_student_grades_example(data_dir_str)?;
    generate_large_dataset_example(data_dir_str)?;
    generate_edge_cases_example(data_dir_str)?;

    println!("Generated all example data files successfully!");
    Ok(())
}

/// Generate simple example with basic data types
fn generate_simple_example(data_dir: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut table = Table::new();

    table.add_column("id".to_string(), ColumnData::Int64(vec![1, 2, 3, 4, 5]))?;

    table.add_column(
        "name".to_string(),
        ColumnData::Varchar(vec![
            "Alice".to_string(),
            "Bob".to_string(),
            "Charlie".to_string(),
            "Diana".to_string(),
            "Eve".to_string(),
        ]),
    )?;

    // Save the table
    let file_path = Path::new(data_dir).join("simple_example.mimdb");
    table.serialize(&file_path)?;

    // Create expected output description
    let expected_content = format!(
        "Simple Example Dataset\n\
         ===================\n\
         Rows: {}\n\
         Columns: {}\n\
         \n\
         Column 'id' (Int64): 5 values from 1 to 5\n\
         Column 'name' (Varchar): 5 names (Alice, Bob, Charlie, Diana, Eve)\n\
         \n\
         File size: {} bytes\n",
        table.row_count,
        table.columns.len(),
        fs::metadata(&file_path)?.len()
    );

    let expected_path = Path::new(data_dir).join("simple_example.txt");
    fs::write(expected_path, expected_content)?;

    Ok(())
}

/// Generate employee dataset example
fn generate_employee_example(data_dir: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut table = Table::new();

    let employee_ids = vec![1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008];
    let salaries = vec![65000, 72000, 58000, 85000, 91000, 67000, 73000, 79000];
    let names = vec![
        "John Smith".to_string(),
        "Sarah Johnson".to_string(),
        "Michael Brown".to_string(),
        "Emily Davis".to_string(),
        "David Wilson".to_string(),
        "Lisa Anderson".to_string(),
        "Robert Taylor".to_string(),
        "Jennifer Martinez".to_string(),
    ];
    let departments = vec![
        "Engineering".to_string(),
        "Marketing".to_string(),
        "Sales".to_string(),
        "Engineering".to_string(),
        "Management".to_string(),
        "Sales".to_string(),
        "Marketing".to_string(),
        "Engineering".to_string(),
    ];

    table.add_column("employee_id".to_string(), ColumnData::Int64(employee_ids))?;
    table.add_column("salary".to_string(), ColumnData::Int64(salaries.clone()))?;
    table.add_column("name".to_string(), ColumnData::Varchar(names))?;
    table.add_column("department".to_string(), ColumnData::Varchar(departments))?;

    let file_path = Path::new(data_dir).join("employee_example.mimdb");
    table.serialize(&file_path)?;

    let avg_salary = salaries.iter().sum::<i64>() as f64 / salaries.len() as f64;
    let expected_content = format!(
        "Employee Dataset\n\
         ===============\n\
         Rows: {}\n\
         Columns: {}\n\
         \n\
         Employee IDs: 1001-1008\n\
         Average Salary: ${:.2}\n\
         Departments: Engineering (3), Marketing (2), Sales (2), Management (1)\n\
         \n\
         File size: {} bytes\n",
        table.row_count,
        table.columns.len(),
        avg_salary,
        fs::metadata(&file_path)?.len()
    );

    let expected_path = Path::new(data_dir).join("employee_example.txt");
    fs::write(expected_path, expected_content)?;

    Ok(())
}

/// Generate sales data example
fn generate_sales_example(data_dir: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut table = Table::new();

    let transaction_ids: Vec<i64> = (2001..2021).collect();
    let amounts = vec![
        150, 275, 89, 450, 320, 125, 680, 95, 380, 220, 540, 175, 295, 410, 85, 625, 190, 355, 480,
        165,
    ];
    let products = vec![
        "Laptop".to_string(),
        "Mouse".to_string(),
        "Keyboard".to_string(),
        "Monitor".to_string(),
        "Tablet".to_string(),
        "Headphones".to_string(),
        "Smartphone".to_string(),
        "Cable".to_string(),
        "Laptop".to_string(),
        "Mouse".to_string(),
        "Webcam".to_string(),
        "Speaker".to_string(),
        "Tablet".to_string(),
        "Monitor".to_string(),
        "Cable".to_string(),
        "Smartphone".to_string(),
        "Keyboard".to_string(),
        "Laptop".to_string(),
        "Webcam".to_string(),
        "Headphones".to_string(),
    ];

    table.add_column(
        "transaction_id".to_string(),
        ColumnData::Int64(transaction_ids),
    )?;
    table.add_column("amount".to_string(), ColumnData::Int64(amounts.clone()))?;
    table.add_column("product".to_string(), ColumnData::Varchar(products))?;

    let file_path = Path::new(data_dir).join("sales_example.mimdb");
    table.serialize(&file_path)?;

    let total_revenue: i64 = amounts.iter().sum();
    let avg_transaction = total_revenue as f64 / amounts.len() as f64;

    let expected_content = format!(
        "Sales Dataset\n\
         ============\n\
         Rows: {}\n\
         Columns: {}\n\
         \n\
         Transaction IDs: 2001-2020\n\
         Total Revenue: ${}\n\
         Average Transaction: ${:.2}\n\
         Product Categories: Electronics (Laptops, Monitors, Tablets, etc.)\n\
         \n\
         File size: {} bytes\n",
        table.row_count,
        table.columns.len(),
        total_revenue,
        avg_transaction,
        fs::metadata(&file_path)?.len()
    );

    let expected_path = Path::new(data_dir).join("sales_example.txt");
    fs::write(expected_path, expected_content)?;

    Ok(())
}

/// Generate student grades example
fn generate_student_grades_example(data_dir: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut table = Table::new();

    let student_ids: Vec<i64> = (3001..3013).collect();
    let math_scores = vec![88, 92, 76, 94, 85, 91, 78, 89, 96, 82, 87, 93];
    let english_scores = vec![91, 87, 89, 88, 92, 85, 94, 86, 90, 95, 83, 89];
    let science_scores = vec![85, 90, 82, 91, 88, 87, 92, 89, 93, 86, 91, 94];

    let student_names = vec![
        "Alex Chen".to_string(),
        "Maria Garcia".to_string(),
        "James Wilson".to_string(),
        "Emma Thompson".to_string(),
        "Oliver Brown".to_string(),
        "Sophia Martinez".to_string(),
        "Lucas Anderson".to_string(),
        "Ava Johnson".to_string(),
        "Noah Davis".to_string(),
        "Isabella Miller".to_string(),
        "William Taylor".to_string(),
        "Mia Moore".to_string(),
    ];

    table.add_column("student_id".to_string(), ColumnData::Int64(student_ids))?;
    table.add_column(
        "math_score".to_string(),
        ColumnData::Int64(math_scores.clone()),
    )?;
    table.add_column(
        "english_score".to_string(),
        ColumnData::Int64(english_scores.clone()),
    )?;
    table.add_column(
        "science_score".to_string(),
        ColumnData::Int64(science_scores.clone()),
    )?;
    table.add_column(
        "student_name".to_string(),
        ColumnData::Varchar(student_names),
    )?;

    let file_path = Path::new(data_dir).join("student_grades_example.mimdb");
    table.serialize(&file_path)?;

    let math_avg = math_scores.iter().sum::<i64>() as f64 / math_scores.len() as f64;
    let english_avg = english_scores.iter().sum::<i64>() as f64 / english_scores.len() as f64;
    let science_avg = science_scores.iter().sum::<i64>() as f64 / science_scores.len() as f64;

    let expected_content = format!(
        "Student Grades Dataset\n\
         =====================\n\
         Rows: {}\n\
         Columns: {}\n\
         \n\
         Student IDs: 3001-3012\n\
         Math Average: {:.1}\n\
         English Average: {:.1}\n\
         Science Average: {:.1}\n\
         \n\
         File size: {} bytes\n",
        table.row_count,
        table.columns.len(),
        math_avg,
        english_avg,
        science_avg,
        fs::metadata(&file_path)?.len()
    );

    let expected_path = Path::new(data_dir).join("student_grades_example.txt");
    fs::write(expected_path, expected_content)?;

    Ok(())
}

/// Generate large dataset for performance testing
fn generate_large_dataset_example(data_dir: &str) -> Result<(), Box<dyn std::error::Error>> {
    // Parameters
    let size: usize = 10_000_000;

    // Prepare file paths
    let file_path = Path::new(data_dir).join("large_dataset_example.mimdb");
    let expected_path = Path::new(data_dir).join("large_dataset_example.txt");

    println!("Generating large dataset with {} rows...", size);

    // Create all data at once - batch processing will be handled internally by serialize
    let ids: Vec<i64> = (1..=size as i64).collect();
    let values: Vec<i64> = (0..size).map(|i| (i as i64 * 17 + 42) % 1000).collect();
    let categories: Vec<String> = (0..size)
        .map(|i| format!("Category_{}", (i % 10) + 1))
        .collect();
    let descriptions: Vec<String> = (0..size)
        .map(|i| {
            format!(
                "Description for item {} with details and additional information",
                i + 1
            )
        })
        .collect();

    let total_sum: i64 = values.iter().sum();

    // Create the complete table
    let mut table = Table::new();
    table.add_column("id".to_string(), ColumnData::Int64(ids))?;
    table.add_column("value".to_string(), ColumnData::Int64(values))?;
    table.add_column("category".to_string(), ColumnData::Varchar(categories))?;
    table.add_column("description".to_string(), ColumnData::Varchar(descriptions))?;

    println!("Serializing with batch processing...");
    // Use default batch processing for memory-efficient serialization
    table.serialize(&file_path)?;

    let avg_value = total_sum as f64 / size as f64;
    let file_size = fs::metadata(&file_path)?.len();

    println!("Large dataset generated successfully!");

    let expected_content = format!(
        "Large Dataset\n\
         ============\n\
         Rows: {}\n\
         Columns: {}\n\
         \n\
         ID Range: 1-{}\n\
         Average Value: {:.2}\n\
         Categories: 10 different categories\n\
         Compression Ratio: Estimated high due to repetitive patterns\n\
         \n\
         File size: {} bytes ({:.2} MB)\n",
        size,
        4,
        size,
        avg_value,
        file_size,
        file_size as f64 / 1024.0 / 1024.0
    );

    fs::write(expected_path, expected_content)?;

    Ok(())
}

/// Generate edge cases dataset
fn generate_edge_cases_example(data_dir: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut table = Table::new();

    let extreme_values = vec![
        i64::MIN,
        i64::MIN + 1,
        -1000000,
        -1,
        0,
        1,
        1000000,
        i64::MAX - 1,
        i64::MAX,
    ];

    let special_strings = vec![
        "".to_string(),                           // Empty
        "🚀".to_string(),                        // Unicode
        "Line1\nLine2".to_string(),              // Newlines
        "Tab\tSeparated".to_string(),            // Tabs
        "Quote\"Inside".to_string(),             // Quotes
        "Very long string that tests the limits of string compression and storage efficiency in the database system with lots of repeated words and patterns".to_string(),
        "MiXeD cAsE TeXt".to_string(),           // Mixed case
        "123456789".to_string(),                 // Numeric string
        "Special!@#$%^&*()".to_string(),         // Special chars
    ];

    table.add_column(
        "extreme_ints".to_string(),
        ColumnData::Int64(extreme_values.clone()),
    )?;
    table.add_column(
        "special_strings".to_string(),
        ColumnData::Varchar(special_strings.clone()),
    )?;

    let file_path = Path::new(data_dir).join("edge_cases_example.mimdb");
    table.serialize(&file_path)?;

    let expected_content = format!(
        "Edge Cases Dataset\n\
         =================\n\
         Rows: {}\n\
         Columns: {}\n\
         \n\
         Extreme integers: MIN={}, MAX={}\n\
         Special strings: Empty, Unicode, Newlines, Tabs, Quotes, Long text\n\
         \n\
         Purpose: Test boundary conditions and edge cases\n\
         File size: {} bytes\n",
        table.row_count,
        table.columns.len(),
        i64::MIN,
        i64::MAX,
        fs::metadata(&file_path)?.len()
    );

    let expected_path = Path::new(data_dir).join("edge_cases_example.txt");
    fs::write(expected_path, expected_content)?;

    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    generate_all_example_files()
}
