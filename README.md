# MIMDB - Columnar Analytical Database Library

MIMDB is a Rust library for working with columnar analytical data storage. It provides a custom columnar file format with compression and optimizations for analytical processing.

## Features

### File Format
- **Two column data types**: 64-bit signed integers (int64) and variable-length strings (VARCHAR)
- **Int64 compression**: Variable Length Encoding (VLE) + Delta Encoding + ZSTD
- **VARCHAR compression**: LZ4 with length prefixes
- **Tabular data**: all columns have the same length (number of rows)
- **File header**: column metadata, versioning, magic bytes

### In-Memory Representation
- **Columnar data structure** optimized for CPU performance
- **Cache-friendly layout** for better data locality
- **Type safety** using Rust enums

### Serialization and Deserialization
- **File writing** with automatic compression
- **File reading** with decompression and validation
- **Data integrity verification**
- **Batch processing** for memory-efficient handling of large datasets
- **Configurable batch sizes** to optimize memory usage vs. performance
- **Streaming decompression** for processing data larger than available RAM

### Analytical Metrics
- **Average values** for int64 columns
- **ASCII character count** for VARCHAR columns
- **Summaries** for each column

## Code Structure

```rust
// Main data types
pub enum ColumnType { Int64, Varchar }
pub enum ColumnData { Int64(Vec<i64>), Varchar(Vec<String>) }
pub struct Table { columns: HashMap<String, ColumnData>, row_count: usize }

// File format
struct FileHeader { version, column_count, row_count, columns }
struct ColumnMeta { name, column_type, compressed_size, uncompressed_size, row_count }
```

## Usage

Add this to your `Cargo.toml`:

```toml
[dependencies]
mimdb = "0.1.0"
```

### Creating a table
```rust
use mimdb::{Table, ColumnData};

let mut table = Table::new();
table.add_column("id".to_string(), ColumnData::Int64(vec![1, 2, 3, 4, 5]))?;
table.add_column("name".to_string(), ColumnData::Varchar(vec![
    "Alice".to_string(),
    "Bob".to_string(),
    "Charlie".to_string(),
    "Diana".to_string(),
    "Eve".to_string()
]))?;
```

### Serialization
```rust
table.serialize("data.mimdb")?;
```

### Deserialization
```rust
let loaded_table = Table::deserialize("data.mimdb")?;
```

### Computing metrics
```rust
let averages = table.calculate_int_averages();
let ascii_counts = table.calculate_ascii_counts();
table.print_metrics();
```

### Batch processing for large datasets
```rust
use mimdb::serialization::BatchConfig;

// Default batch processing (100k rows per batch)
table.serialize_with_config("large_data.mimdb", &BatchConfig::default())?;

// Custom batch size for memory-constrained environments
let config = BatchConfig::new(50_000);
table.serialize_with_config("large_data.mimdb", &config)?;

// Load with batch processing
let loaded_table = Table::deserialize_with_config("large_data.mimdb", &config)?;
```

## Building and Running

```bash
# Build the library
cargo build

# Run tests
cargo test

# Run the simple usage example
cargo run --example simple_usage

# Run the data analysis example
cargo run --example data_analysis

# Run the batch processing example (handles large datasets)
cargo run --example batch_processing
```

## Tools

### MIMDB File Loader

The `loader` utility allows you to inspect and analyze existing MIMDB files from the command line.

```bash
# Build the loader tool
cargo build --bin loader

# Analyze a MIMDB file
./target/debug/loader examples/data/simple_example.mimdb
```

## Examples

The library includes three examples in the `examples/` directory:

1. **`simple_usage.rs`** - Comprehensive demonstration showing all library features including table creation, serialization, deserialization, and data integrity verification
2. **`data_analysis.rs`** - Advanced analytics example demonstrating statistical analysis and character analysis capabilities
3. **`batch_processing.rs`** - Demonstrates memory-efficient processing of large datasets (10M+ rows) using configurable batch sizes

See the [examples README](examples/README.md) for detailed information about each example.

## License

This project is intended for educational use only by employees and students of MIMUW.