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
table.add_column("name".to_string(), ColumnData::Varchar(vec!["Alice".to_string(), "Bob".to_string()]))?;
```

### Serialization
```rust
table.save_to_file("data.mimdb")?;
```

### Deserialization
```rust
let loaded_table = Table::load_from_file("data.mimdb")?;
```

### Computing metrics
```rust
let averages = table.calculate_int_averages();
let ascii_counts = table.calculate_ascii_counts();
table.print_metrics();
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
```

## Tools

### MIMDB File Loader

The `loader` utility allows you to inspect and analyze existing MIMDB files from the command line.

```bash
# Build the loader tool
cargo build --bin loader

# Analyze a MIMDB file
./target/debug/loader examples/data/simple_example.mimdb

# Example output:
# Loading MIMDB file: examples/data/simple_example.mimdb
# ✓ Successfully loaded table from examples/data/simple_example.mimdb
#
# === FILE INFORMATION ===
# File: examples/data/simple_example.mimdb
#
# === TABLE METRICS ===
# Total rows: 5
# Total columns: 2
#
# Integer column averages:
#   id: 3.0000
#
# Varchar column ASCII character counts:
#   name: 23 total ASCII characters
#
# === COLUMN DETAILS ===
#   name (Varchar): 5 rows
#   id (Int64): 5 rows
```

The loader provides:
- **File validation** - verifies MIMDB format and loads successfully
- **Table metrics** - displays row/column counts and statistical analysis
- **Column information** - shows data types and sizes for each column
- **Error handling** - clear error messages for invalid files or paths

## Examples

The library includes two examples in the `examples/` directory:

1. **`simple_usage.rs`** - Comprehensive demonstration showing all library features including table creation, serialization, deserialization, and data integrity verification
2. **`data_analysis.rs`** - Advanced analytics example demonstrating statistical analysis and character analysis capabilities

## Examples Overview

### Simple Usage Example
The `simple_usage.rs` example creates a comprehensive sample table and demonstrates:
1. Table creation with multiple column types
2. Serialization to file `comprehensive_example.mimdb`
3. Calculation and display of detailed metrics
4. Deserialization from file
5. Data integrity verification
6. Comparison of metrics before and after serialization

### Data Analysis Example
The `data_analysis.rs` example focuses on analytical capabilities and demonstrates:
1. Creating tables optimized for analysis
2. Advanced statistical analysis of numeric columns
3. Character frequency analysis for text columns
4. Detailed data exploration and visualization

## Technologies

- **Rust 2024 Edition** - memory safety and performance
- **ZSTD** - compression for numeric data
- **LZ4** - fast compression for text data
- **Serde + Bincode** - metadata serialization
- **Anyhow** - error handling

## Optimizations

1. **Delta Encoding** - reduces size of numeric data with similar values
2. **Variable Length Encoding** - reduces size of small numbers
3. **Zigzag Encoding** - efficient encoding of negative numbers
4. **Columnar layout** - better memory locality for analytical operations
5. **Algorithmic compression** - ZSTD for numbers, LZ4 for text

## Performance

Example results for a 10x5 table:
- **Uncompressed size**: ~400 bytes of data + metadata
- **File size**: 575 bytes (includes header, metadata and compressed data)
- **Compression**: ~30% space savings for example data

## Extensions

The project can be extended with:
- Additional data types (float, boolean, timestamp)
- Column indexes
- Parallel processing
- REST API endpoints
- Advanced statistical metrics
- Query language

## License

This project is intended for educational use only by employees and students of MIMUW.