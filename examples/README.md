# MIMDB Examples

The library includes three examples demonstrating various features and capabilities.

## Available Examples

1. **`simple_usage.rs`** - Comprehensive demonstration showing all library features including table creation, serialization, deserialization, and data integrity verification
2. **`data_analysis.rs`** - Advanced analytics example demonstrating statistical analysis and character analysis capabilities
3. **`batch_processing.rs`** - Demonstrates memory-efficient processing of large datasets (10M+ rows) using configurable batch sizes

## Simple Usage Example

The `simple_usage.rs` example creates a comprehensive sample table and demonstrates:
1. Table creation with multiple column types
2. Serialization to file `comprehensive_example.mimdb`
3. Calculation and display of detailed metrics
4. Deserialization from file
5. Data integrity verification
6. Comparison of metrics before and after serialization

## Data Analysis Example

The `data_analysis.rs` example focuses on analytical capabilities and demonstrates:
1. Creating tables optimized for analysis
2. Advanced statistical analysis of numeric columns
3. Character frequency analysis for text columns
4. Detailed data exploration and visualization

## Batch Processing Example

The `batch_processing.rs` example demonstrates handling large datasets and shows:
1. Creating large tables (10 million rows) for performance testing
2. Comparing different batch sizes (10k, 100k, 500k rows) and their impact on performance
3. Memory-efficient serialization and deserialization using `BatchConfig`
4. Performance benchmarking of serialization/deserialization times
5. File size analysis and compression effectiveness on large datasets
6. Data integrity verification across all batch configurations

## Running Examples

```bash
# Run simple usage example
cargo run --example simple_usage

# Run data analysis example
cargo run --example data_analysis

# Run batch processing example
cargo run --example batch_processing
```

## Example Data

The `data/` directory contains sample `.mimdb` files and corresponding `.txt` descriptions for testing and demonstration purposes.
