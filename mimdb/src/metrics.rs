/*
 * Copyright (c) 2025-present Dawid Pawlik
 *
 * For educational use only by employees and students of MIMUW.
 * See LICENSE file for details.
 */

//! # Metrics and analysis functions for MIMDB tables
//!
//! This module provides analytical functions for computing statistics
//! and metrics on columnar data stored in Table structures.

use crate::ColumnData;
use crate::Table;
use std::collections::HashMap;

impl Table {
    /// Calculate average for all integer columns
    pub fn calculate_int_averages(&self) -> HashMap<String, f64> {
        let mut averages = HashMap::new();

        for (name, column) in &self.columns {
            if let ColumnData::Int64(data) = column
                && !data.is_empty()
            {
                let sum: i64 = data.iter().sum();
                let average = sum as f64 / data.len() as f64;
                averages.insert(name.clone(), average);
            }
        }

        averages
    }

    /// Count ASCII characters for all varchar columns
    pub fn calculate_ascii_counts(&self) -> HashMap<String, HashMap<char, usize>> {
        let mut char_counts = HashMap::new();

        for (name, column) in &self.columns {
            if let ColumnData::Varchar(data) = column {
                let mut counts = HashMap::new();

                for string in data {
                    for ch in string.chars() {
                        if ch.is_ascii() {
                            *counts.entry(ch).or_insert(0) += 1;
                        }
                    }
                }

                char_counts.insert(name.clone(), counts);
            }
        }

        char_counts
    }

    /// Get total ASCII character count for a varchar column
    pub fn get_total_ascii_count(&self, column_name: &str) -> Option<usize> {
        if let Some(ColumnData::Varchar(data)) = self.get_column(column_name) {
            let total = data
                .iter()
                .flat_map(|s| s.chars())
                .filter(|&c| c.is_ascii())
                .count();
            Some(total)
        } else {
            None
        }
    }

    /// Print metrics for the table
    pub fn print_metrics(&self) {
        println!("\n=== TABLE METRICS ===");
        println!("Total rows: {}", self.row_count);
        println!("Total columns: {}", self.columns.len());

        // Integer column averages
        let averages = self.calculate_int_averages();
        if !averages.is_empty() {
            println!("\nInteger column averages:");
            for (name, avg) in &averages {
                println!("  {}: {:.4}", name, avg);
            }
        }

        // ASCII character counts for varchar columns
        let char_counts = self.calculate_ascii_counts();
        if !char_counts.is_empty() {
            println!("\nVarchar column ASCII character counts:");
            for name in char_counts.keys() {
                if let Some(total) = self.get_total_ascii_count(name) {
                    println!("  {}: {} total ASCII characters", name, total);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ColumnData;

    #[test]
    fn test_metrics_calculation() {
        let mut table = Table::new();

        table
            .add_column("scores".to_string(), ColumnData::Int64(vec![80, 90, 100]))
            .unwrap();
        table
            .add_column(
                "names".to_string(),
                ColumnData::Varchar(vec![
                    "ABC".to_string(),
                    "DEF".to_string(),
                    "GHI".to_string(),
                ]),
            )
            .unwrap();

        let averages = table.calculate_int_averages();
        assert_eq!(averages.get("scores"), Some(&90.0));

        let ascii_counts = table.calculate_ascii_counts();
        assert!(ascii_counts.contains_key("names"));

        let total_ascii = table.get_total_ascii_count("names").unwrap();
        assert_eq!(total_ascii, 9); // "ABC" + "DEF" + "GHI" = 9 ASCII chars
    }
}
